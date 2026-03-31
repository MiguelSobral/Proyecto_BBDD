from pymongo import MongoClient
import json
import pymysql
from datetime import datetime
import configuracion as cf




def conexion_mysql(db=None):

    return pymysql.connect(host=cf.MYSQL_HOST, user=cf.USER, password=cf.PASSWORD, database=db, autocommit=False)


def conexion_mongodb():
    client =  MongoClient(cf.CONNECTION_STRING)
    dbname = client[cf.MONGO_DATABASE]
    colection = dbname[cf.MONGO_COLLECTION]

    return colection


def crear_db_tabla_sql():
    conexion = conexion_mysql()
    with conexion:
        cursor = conexion.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS " + str(cf.MYSQL_DATABASE)
        cursor.execute(sql)

        sql = "USE " + str(cf.MYSQL_DATABASE)
        cursor.execute(sql)

        cursor.execute(cf.SQL_TABLA_USUARIOS)
        cursor.execute(cf.SQL_TABLA_PRODUCTOS)
        cursor.execute(cf.SQL_TABLA_REVIEWS)

        conexion.commit()


def obtener_tipo_producto(ruta_fichero):
    if "Video_Games" in ruta_fichero:
        return "Video_Games"
    
    elif "Toys_and_Games" in ruta_fichero:
        return "Toys_and_Games"
    
    elif "Digital_Music" in ruta_fichero:
        return "Digital_Music"
    
    elif "Musical_Instruments" in ruta_fichero:
        return "Musical_Instruments"
    
    else:
        return "No se sabe"
    

def insertar_lote_mysql(cursor, filas_usuarios, filas_productos, filas_reviews):
    if filas_usuarios:
        sql_usuarios = """
            INSERT IGNORE INTO usuarios (reviewerID, reviewerName)
            VALUES (%s, %s);
        """
        cursor.executemany(sql_usuarios, filas_usuarios)

    if filas_productos:
        sql_productos = """
            INSERT IGNORE INTO productos (asin, tipo_producto)
            VALUES (%s, %s);
        """
        cursor.executemany(sql_productos, filas_productos)

    if filas_reviews:
        sql_reviews = """
            INSERT IGNORE INTO reviews
            (reviewerID, asin, tipo_producto, overall, unixReviewTime, reviewTime, helpful_1, helpful_2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(sql_reviews, filas_reviews)


def inserta_datos_mysql(ruta_fichero):
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    tipo_producto = obtener_tipo_producto(ruta_fichero)

    with conexion:
        cursor = conexion.cursor()

        filas_usuarios = []
        filas_productos = []
        filas_reviews = []

        with open(ruta_fichero, "r", encoding="utf-8") as f:
            for review in f:
                line = json.loads(review.strip())

                reviewerID = line.get("reviewerID")
                reviewerName = line.get("reviewerName")
                asin = line.get("asin")   
                overall = line.get("overall")
                helpful_1 = line.get("helpful")[0]
                helpful_2 = line.get("helpful")[1]
                unixReviewTime = line.get("unixReviewTime")

                fecha_texto = line.get("reviewTime")
                reviewTime = datetime.strptime(fecha_texto, "%m %d, %Y").date()

                filas_usuarios.append((reviewerID, reviewerName))
                filas_productos.append((asin, tipo_producto))
                filas_reviews.append((reviewerID, asin, tipo_producto, overall, unixReviewTime, reviewTime, helpful_1, helpful_2))

                if len(filas_usuarios) == cf.MAX_LOTE:
                    insertar_lote_mysql(cursor, filas_usuarios, filas_productos, filas_reviews)
                    conexion.commit()

                    filas_usuarios.clear()
                    filas_productos.clear()
                    filas_reviews.clear()
        
        insertar_lote_mysql(cursor, filas_usuarios, filas_productos, filas_reviews)

        conexion.commit()


def inserta_mongodb(ruta_fichero):
    collection = conexion_mongodb()

    tipo_producto = obtener_tipo_producto(ruta_fichero)

    elementos_insertar = []

    with open(ruta_fichero, "r", encoding="utf-8") as file:
        for review in file:
            line = json.loads(review.strip())

            reviewerID = line.get("reviewerID")
            asin = line.get("asin")   
            summary = line.get("summary")
            reviewText = line.get("reviewText")

            elemento = { "reviewerID": reviewerID, "asin": asin, "tipo_producto": tipo_producto, "summary": summary, "reviewText": reviewText}

            elementos_insertar.append(elemento)

            if len(elementos_insertar) == cf.MAX_LOTE:
                collection.insert_many(elementos_insertar)
                
                elementos_insertar.clear()

    if elementos_insertar:
        collection.insert_many(elementos_insertar)


def main():
    crear_db_tabla_sql()

    for ruta in cf.RUTAS:
        inserta_datos_mysql(ruta)
        inserta_mongodb(ruta)




if __name__ == "__main__":
    main()