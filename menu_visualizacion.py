from load_data import conexion_mongodb, conexion_mysql
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
import configuracion as cf




def elegir_categoria():
    categoria = input("Elige la categoria (Video games, Toys and games, Digital music, Musical instruments, todos): ").lower()

    while categoria not in cf.CATEGORIAS_VALIDAS:
        print("No has introducido una categoria valida")
        categoria = input("Elige la categoria (Video games, Toys and games, Digital music, Musical instruments, todos): ").lower()

    return categoria


def grafica_1():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    categoria = elegir_categoria()

    with conexion:
        cursor = conexion.cursor()

        if categoria == "todos":
            sql = """
                SELECT YEAR(reviewTime) AS anio, count(review_id) AS total_reviews
                FROM reviews
                GROUP BY anio
                ORDER BY anio ASC;
            """
            cursor.execute(sql)

        else:
            mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)
            sql = """
                SELECT YEAR(r.reviewTime) AS anio, count(r.review_id) AS total_reviews
                FROM reviews r
                    INNER JOIN productos p ON r.id_producto = p.id_producto
                WHERE p.tipo_producto = %s
                GROUP BY anio
                ORDER BY anio ASC;
            """
            cursor.execute(sql, (mapeo_categoria,))

        datos = cursor.fetchall()
    
    titulo = f"Reviews por año de {categoria}"

    year = [dato[0] for dato in datos]
    total_reviews = [dato[1] for dato in datos]

    plt.figure(figsize=(10, 6))
    plt.bar(year, total_reviews)
    plt.title(titulo)
    plt.xlabel("Años")
    plt.ylabel("Numero de reviews")
    plt.xticks(year)
    plt.show()


def grafica_2():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    categoria = elegir_categoria()

    with conexion:
        cursor = conexion.cursor()

        if categoria == "todos":
            sql = """
                SELECT p.asin, p.tipo_producto, COUNT(r.review_id) AS total_reviews
                FROM reviews r
                    INNER JOIN productos p ON r.id_producto = p.id_producto
                GROUP BY p.asin, p.tipo_producto
                ORDER BY total_reviews DESC;
            """
            cursor.execute(sql)

        else:
            mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)
            sql = """
                SELECT p.asin, p.tipo_producto, COUNT(r.review_id) AS total_reviews
                FROM reviews r
                    INNER JOIN productos p ON r.id_producto = p.id_producto
                WHERE p.tipo_producto = %s
                GROUP BY p.asin
                ORDER BY total_reviews DESC;
            """
            cursor.execute(sql, (mapeo_categoria,))

        datos = cursor.fetchall()

    titulo = f"Evolucion de popularidad de {categoria}"
    
    popularidad = [dato[2] for dato in datos]

    ranking = list(range(1, len(popularidad) + 1))

    plt.figure(figsize=(10, 6))
    plt.plot(ranking, popularidad)
    plt.title(titulo)
    plt.xlabel("Articulos")
    plt.ylabel("Numero de reviews")
    plt.show()


def posibles_cat_asin(asin):
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            select asin, tipo_producto
            FROM productos
            WHERE asin = %s
        """
        cursor.execute(sql, (asin,))

        datos = cursor.fetchall()

    return [dato[1] for dato in datos]


def grafica_3():
    print(cf.OPCIONES_HISTOGRAMA)
    opcion = input("Opcion: ")
    while opcion not in cf.OPCIONES_HISTOGRAMA_POSIBLES:
        print("No es una opcion valida")
        opcion = input("Opcion (1, 2): ")

    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        if opcion == "1":
            categoria = elegir_categoria()

            if categoria == "todos":
                sql = """
                    SELECT overall, COUNT(review_id) AS total_reviews
                    FROM reviews
                    GROUP BY overall
                    ORDER BY overall ASC;
                """
                cursor.execute(sql)

            else:
                mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)
                sql = """
                    SELECT r.overall, COUNT(r.review_id) AS total_reviews
                    FROM reviews r
                        INNER JOIN productos p ON r.id_producto = p.id_producto
                    WHERE p.tipo_producto = %s
                    GROUP BY overall
                    ORDER BY overall ASC;
                """
                cursor.execute(sql, (mapeo_categoria,))

            titulo = f"Reviews por nota de {categoria}"

        else:
            asin = input("Introduce el asin del articulo: ")
            posibles_categorias = posibles_cat_asin(asin)

            if len(posibles_categorias) == 0:
                print("Ese asticulo no existe")
                return

            elif len(posibles_categorias) == 1:
                sql = """
                    SELECT r.overall, COUNT(review_id) AS total_reviews
                    FROM reviews r
                        INNER JOIN productos p ON r.id_producto = p.id_producto
                    WHERE p.asin = %s
                    GROUP BY overall
                    ORDER BY overall ASC;
                """
                cursor.execute(sql, (asin,))

                titulo = f"Histograma por nota del artículo {asin}"

            else:
                cat_validas = [i.replace("_", " ").lower() for i in posibles_categorias]
                categoria = input(f"Hay varios tipos de productos con ese asin, elige {cat_validas}: ").lower()
                while categoria not in cat_validas:
                    print("Esa no es una de las categorias validas")
                    categoria = input(f"Hay varios tipos de productos con ese asin, elige {cat_validas}: ").lower()

                mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)

                sql = """
                    SELECT r.overall, COUNT(review_id) AS total_reviews
                    FROM reviews r
                        INNER JOIN productos p ON r.id_producto = p.id_producto
                    WHERE p.asin = %s
                        AND p.tipo_producto = %s
                    GROUP BY overall
                    ORDER BY overall ASC;
                """
                cursor.execute(sql, (asin, mapeo_categoria))
            
                titulo = f"Histograma por nota del artículo {asin} ({categoria})"

        datos = cursor.fetchall()

    overall = [dato[0] for dato in datos]
    total_reviews = [dato[1] for dato in datos]

    plt.figure(figsize=(10, 6))
    plt.bar(overall, total_reviews)
    plt.title(titulo)
    plt.xlabel("Overall")
    plt.ylabel("Numero de reviews")
    plt.xticks(overall)
    plt.show()


def grafica_4():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            SELECT p.tipo_producto, r.unixReviewTime
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            ORDER BY p.tipo_producto, unixReviewTime;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    reviews = {"Video_Games": 0, "Toys_and_Games": 0, "Digital_Music": 0, "Musical_Instruments": 0}
    series_grafica = {"Video_Games": [], "Toys_and_Games": [], "Digital_Music": [], "Musical_Instruments": []}

    for categoria, unixtime in datos:
        reviews[categoria] += 1
        series_grafica[categoria].append((unixtime, reviews[categoria]))

    plt.figure(figsize=(10, 6))
    for categoria, tupla in series_grafica.items():
        x = [t[0] for t in tupla]
        y = [t[1] for t in tupla]
        plt.plot(x, y, label=categoria)
    plt.title("Evolución de las reviews a lo largo del tiempo de todos los productos")
    plt.xlabel("Tiempo")
    plt.ylabel("Numero de reviews hasta ese momento")
    plt.legend()
    plt.show()


def grafica_5():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            SELECT ReviewerID, COUNT(review_id) AS total_reviews
            FROM reviews
            GROUP BY ReviewerID;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    reviews_por_user = [dato[1] for dato in datos]
    numero_users_por_cant_reviews = Counter(reviews_por_user)

    x = sorted(numero_users_por_cant_reviews.keys())
    y = [numero_users_por_cant_reviews[key] for key in x]

    plt.figure(figsize=(10, 6))
    plt.bar(x, y)
    plt.title("Reviews por usuario")
    plt.xlabel("Numero de reviews")
    plt.ylabel("Numero de usuarios")
    plt.show()


def grafica_6():
    coleccion = conexion_mongodb()

    categoria = elegir_categoria()
    mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)
    titulo = f"Nube de palabras de {categoria}"

    if categoria == "todos":
        datos = coleccion.find({}, {"_id": 0, "summary": 1})
    else:
        datos = coleccion.find({"tipo_producto": mapeo_categoria}, {"_id": 0, "summary": 1})
    
    resumenes = [dato["summary"] for dato in datos]
    resumenes_concatenados = " ".join(resumenes)

    palabras = resumenes_concatenados.split(" ")
    palabras = [palabra for palabra in palabras if len(palabra) > 3]

    palabras_concatenadas = " ".join(palabras)

    nube = WordCloud().generate(palabras_concatenadas)

    plt.figure(figsize=(10, 6))
    plt.imshow(nube)
    plt.axis("off")
    plt.title(titulo)
    plt.show()


def grafica_7():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            SELECT p.tipo_producto, AVG(r.overall) AS media_overall
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            GROUP BY p.tipo_producto
            ORDER BY media_overall ASC;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    categorias = [dato[0] for dato in datos]
    media_overall = [dato[1] for dato in datos]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(categorias, media_overall)
    plt.bar_label(bars, fmt="%.5f")
    plt.title("Promedio overall por categoria")
    plt.xlabel("Categoria")
    plt.ylabel("Promedio Overall")
    plt.show()


def main():
    print(cf.MENU)

    opcion = 0
    while opcion != "8":
        opcion = input("Elige una opcion: ")
        while opcion not in cf.OPCIONES_MENU:
            print("Esa no es una opcion valida")
            opcion = input("Elige una opcion (1-8): ")

        if opcion == "1":
            grafica_1()
        elif opcion == "2":
            grafica_2()
        elif opcion == "3":
            grafica_3()
        elif opcion == "4":
            grafica_4()
        elif opcion == "5":
            grafica_5()
        elif opcion == "6":
            grafica_6()
        elif opcion == "7":
            grafica_7()




if __name__ == "__main__":
    main()