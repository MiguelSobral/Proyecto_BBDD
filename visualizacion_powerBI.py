from load_data import conexion_mysql
import matplotlib.pyplot as plt
import configuracion as cf
import pandas as pd
import os




def exportar_reviews_por_anio():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    sql = """
        SELECT YEAR(reviewTime) AS anio, count(review_id) AS total_reviews
        FROM reviews
        GROUP BY anio
        ORDER BY anio ASC;
    """
    df = pd.read_sql(sql, conexion)

    df.to_csv("CSVs_powerBI/reviews_por_anio.csv", index=False)


def exportar_popularidad_articulos():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    sql = """
        SELECT p.asin, p.tipo_producto, COUNT(r.review_id) AS total_reviews
        FROM reviews r
            INNER JOIN productos p ON r.id_producto = p.id_producto
        GROUP BY p.asin, p.tipo_producto
        ORDER BY total_reviews DESC;
    """
    df = pd.read_sql(sql, conexion)

    df.insert(0, "ranking", range(1, len(df) + 1))
    df.to_csv("CSVs_powerBI/popularidad_articulos.csv", index=False)


def exportar_reviews_por_usuarios():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    sql = """
        SELECT ReviewerID, COUNT(review_id) AS total_reviews
        FROM reviews
        GROUP BY ReviewerID;
    """
    df = pd.read_sql(sql, conexion)

    df.to_csv("CSVs_powerBI/reviews_por_usuarios.csv", index=False)


def recomendar_articulos(reviewerID, tipo_producto):
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        sql = """
            SELECT p.id_producto, COUNT(r.review_id) AS popularidad
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE p.tipo_producto = %s
                AND p.id_producto NOT IN (
                    SELECT r2.id_producto
                    FROM reviews r2
                        INNER JOIN productos p2 ON r2.id_producto = p2.id_producto
                    WHERE r2.reviewerID = %s
                        AND p2.tipo_producto = %s
            )
            GROUP BY p.id_producto
            ORDER BY popularidad DESC, p.id_producto ASC
            LIMIT 10;
        """
        cursor.execute(sql, (tipo_producto, reviewerID, tipo_producto))
        
        datos = cursor.fetchall()

    return [dato[0] for dato in datos]


def main():
    if not os.exists("CSVs_powerBI"):
        os.makedirs("CSVs_powerBI", exist_ok=True)
        exportar_reviews_por_anio()
        exportar_popularidad_articulos()
        exportar_reviews_por_usuarios()
        print("Se han generado los CSV para PowerBI")

    




if __name__ == "__main__":
    main()