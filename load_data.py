from pymongo import MongoClient
import json
import pymysql
from datetime import datetime
import configuracion as cf




def conexion_mysql(db=None):
    """
    Función para establecer una conexión con la base de datos MySQL.
    """
    return pymysql.connect(host=cf.MYSQL_HOST, user=cf.USER, password=cf.PASSWORD, database=db, autocommit=False)


def conexion_mongodb():
    """
    Función para establecer una conexión con la base de datos MongoDB.
    """
    client =  MongoClient(cf.CONNECTION_STRING)
    dbname = client[cf.MONGO_DATABASE]
    colection = dbname[cf.MONGO_COLLECTION]

    return colection


def crear_db_tabla_sql():
    """
    Función para crear la base de datos y las tablas necesarias en MySQL.
    """
    conexion = conexion_mysql() # Conexion sin especificar la base de datos para poder crearla
    with conexion:
        cursor = conexion.cursor() 

        # Crear la base de datos si no existe
        sql = "CREATE DATABASE IF NOT EXISTS " + str(cf.MYSQL_DATABASE)
        cursor.execute(sql)

        # Usar la base de datos creada
        sql = "USE " + str(cf.MYSQL_DATABASE)
        cursor.execute(sql)

        # Crear las tablas necesarias
        cursor.execute(cf.SQL_TABLA_USUARIOS)
        cursor.execute(cf.SQL_TABLA_PRODUCTOS)
        cursor.execute(cf.SQL_TABLA_REVIEWS)

        # Confirmar los cambios en la base de datos
        conexion.commit()


def obtener_tipo_producto(ruta_fichero):
    """
    Función para determinar el tipo de producto a partir del nombre del fichero.
    """
    # Dependiendo del nombre del fichero, se asigna un tipo de producto específico. Si no se encuentra ningún tipo, se asigna "No se sabe".
    if "Video_Games" in ruta_fichero:
        return "Video_Games"
    
    elif "Toys_and_Games" in ruta_fichero:
        return "Toys_and_Games"
    
    elif "Digital_Music" in ruta_fichero:
        return "Digital_Music"
    
    elif "Musical_Instruments" in ruta_fichero:
        return "Musical_Instruments"
    
    elif "Sports_and_Outdoors" in ruta_fichero:
        return "Sports_and_Outdoors"
    
    else:
        return "No se sabe"
    

def insertar_lote_mysql(cursor, filas_usuarios, filas_productos, filas_reviews):
    """
    Función para insertar lotes de datos en las tablas de MySQL.
    """
    # Si hay filas para insertar en la tabla de usuarios, se insertan los datos en la tabla de usuarios.
    if filas_usuarios:
        sql_usuarios = """
            INSERT IGNORE INTO usuarios (reviewerID, reviewerName)
            VALUES (%s, %s);
        """
        cursor.executemany(sql_usuarios, filas_usuarios)

    # Si hay filas para insertar en la tabla de productos, se insertan los datos en la tabla de productos.
    if filas_productos:
        sql_productos = """
            INSERT IGNORE INTO productos (asin, tipo_producto)
            VALUES (%s, %s);
        """
        cursor.executemany(sql_productos, filas_productos)

    # Si hay filas para insertar en la tabla de reviews, se insertan los datos en la tabla de reviews.
    if filas_reviews:
        sql_reviews = """
            INSERT IGNORE INTO reviews
            (reviewerID, id_producto, overall, unixReviewTime, reviewTime, helpful_1, helpful_2)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(sql_reviews, filas_reviews)


def id_productos(cursor, asin, tipo_producto):
    """
    Funcion para obtener el id_producto a partir del asin y tipo_producto.
    """    
    sql = """
        SELECT id_producto
        FROM productos
        WHERE asin = %s 
            AND tipo_producto = %s
    """
    cursor.execute(sql, (asin, tipo_producto))

    # Devolvemos el id_producto obtenido de la consulta SQL. Si no se encuentra ningún producto, se devuelve None.
    return cursor.fetchone()[0]


def inserta_datos_mysql(ruta_fichero):
    """
    Función para insertar los datos de los ficheros JSON en las tablas de MySQL.
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE) # Conexion especificando la base de datos para poder insertar los datos en las tablas correspondientes

    # Obtener el tipo de producto a partir del nombre del fichero.
    tipo_producto = obtener_tipo_producto(ruta_fichero)

    with conexion:
        cursor = conexion.cursor()

        # Creamos las listas para almacenar los datos de los usuarios y productos que se van a insertar en las tablas correspondientes.
        filas_usuarios = []
        filas_productos = []

        # Primera pasada para insertar los datos en las tablas de usuarios y productos.
        with open(ruta_fichero, "r", encoding="utf-8") as f:
            for review in f:
                line = json.loads(review.strip()) # Para cada línea del fichero, se convierte la línea de texto en un diccionario utilizando json.loads().

                # Cogemos los datos necesarios para insertar en las tablas de usuarios y productos. Si no se encuentra algún dato, se asigna None.
                reviewerID = line.get("reviewerID")
                reviewerName = line.get("reviewerName")
                asin = line.get("asin")   

                # Añadimos los datos obtenidos a las listas correspondientes en forma de tuplas.
                filas_usuarios.append((reviewerID, reviewerName))
                filas_productos.append((asin, tipo_producto))

                # Si se han procesado un número de filas igual al tamaño máximo, se insertan los datos en las tablas de MySQL y se vacian las listas.
                if len(filas_usuarios) == cf.MAX_LOTE:
                    insertar_lote_mysql(cursor, filas_usuarios, filas_productos, [])
                    conexion.commit()

                    filas_usuarios.clear()
                    filas_productos.clear()

        # Insertamos los datos restantes en las tablas de MySQL después de procesar todas las líneas del fichero.
        insertar_lote_mysql(cursor, filas_usuarios, filas_productos, [])
        conexion.commit()

        # Creamos la lista para almacenar los datos de las reviews que se van a insertar en la tabla de reviews.
        filas_reviews = []

        # Segunda pasada para insertar los datos en la tabla de reviews y poder usar el id_producto obtenido a partir del asin y tipo_producto.
        with open(ruta_fichero, "r", encoding="utf-8") as f:
            for review in f:
                line = json.loads(review.strip()) # Para cada línea del fichero, se convierte la línea de texto en un diccionario utilizando json.loads().
                
                # Cogemos los datos necesarios para insertar en la tabla de reviews. Si no se encuentra algún dato, se asigna None.
                reviewerID = line.get("reviewerID")
                asin = line.get("asin")   
                overall = line.get("overall")
                helpful_1 = line.get("helpful")[0]
                helpful_2 = line.get("helpful")[1]
                unixReviewTime = line.get("unixReviewTime")

                # La fecha la pasamos al formato correcto.
                fecha_texto = line.get("reviewTime")
                reviewTime = datetime.strptime(fecha_texto, "%m %d, %Y").date()

                # Obtenemos el id_producto.
                id_producto = id_productos(cursor, asin, tipo_producto)

                # Añadimos los datos obtenidos a la lista correspondiente en forma de tuplas.
                filas_reviews.append((reviewerID, id_producto, overall, unixReviewTime, reviewTime, helpful_1, helpful_2))

                # Si se han procesado un número de filas igual al tamaño máximo, se insertan los datos en la tabla de MySQL y se vacia la lista.
                if len(filas_reviews) == cf.MAX_LOTE:
                    insertar_lote_mysql(cursor, [], [], filas_reviews)
                    conexion.commit()

                    filas_reviews.clear()
        
        # Insertamos los datos restantes en la tabla de MySQL después de procesar todas las líneas del fichero.
        insertar_lote_mysql(cursor, [], [], filas_reviews)
        conexion.commit()


def inserta_mongodb(ruta_fichero):
    """
    Funcion para insertar los datos de los ficheros JSON en la colección de MongoDB.
    """
    collection = conexion_mongodb()

    # Obtener el tipo de producto a partir del nombre del fichero.
    tipo_producto = obtener_tipo_producto(ruta_fichero)

    # Creamos la lista para almacenar los datos de las reviews que se van a insertar en la colección de MongoDB.
    elementos_insertar = []

    with open(ruta_fichero, "r", encoding="utf-8") as file:
        for review in file:
            line = json.loads(review.strip()) # Para cada línea del fichero, se convierte la línea de texto en un diccionario utilizando json.loads().

            # Cogemos los datos necesarios para insertar en la colección de MongoDB. Si no se encuentra algún dato, se asigna None.
            reviewerID = line.get("reviewerID")
            asin = line.get("asin")   
            summary = line.get("summary")
            reviewText = line.get("reviewText")

            # Creamos el documento para insertar en la colección de MongoDB. Si no se encuentra algún dato, se asigna None. 
            elemento = { "reviewerID": reviewerID, "asin": asin, "tipo_producto": tipo_producto, "summary": summary, "reviewText": reviewText}

            # Añadimos el documento a la lista de elementos a insertar.
            elementos_insertar.append(elemento)

            # Si se han procesado un número de elementos igual al tamaño máximo, se insertan los documentos en la colección de MongoDB y se vacia la lista.
            if len(elementos_insertar) == cf.MAX_LOTE:
                collection.insert_many(elementos_insertar)
                
                elementos_insertar.clear()

    # Insertamos los documentos restantes en la colección de MongoDB después de procesar todas las líneas del fichero.
    if elementos_insertar:
        collection.insert_many(elementos_insertar)


def main():
    """
    Función principal que ejecuta el proceso de creación de la base de datos, tablas e inserción de datos en MySQL y MongoDB.
    """
    crear_db_tabla_sql()

    for ruta in cf.RUTAS:
        inserta_datos_mysql(ruta)
        inserta_mongodb(ruta)




if __name__ == "__main__":
    main()