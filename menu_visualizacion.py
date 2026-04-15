from load_data import conexion_mongodb, conexion_mysql
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
import configuracion as cf




def elegir_categoria():
    """
    Funcion que pide al usuario que elija una categoria y se asegura de que la categoria introducida es valida
    """
    # Se pide al usuario que elija una categoria
    categoria = input("Elige la categoria (Video games, Toys and games, Digital music, Musical instruments, todos): ").lower()

    # Se asegura de que la categoria introducida es valida
    while categoria not in cf.CATEGORIAS_VALIDAS:
        print("No has introducido una categoria valida")
        categoria = input("Elige la categoria (Video games, Toys and games, Digital music, Musical instruments, todos): ").lower()

    return categoria


def grafica_1():
    """
    Funcion que muestra un grafico de barras con el numero de reviews por año, se puede elegir una categoria o mostrar todas las categorias
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Se pide al usuario que elija una categoria
    categoria = elegir_categoria()

    with conexion:
        cursor = conexion.cursor()

        # Si la categoria es "todos" se muestra el numero de reviews por año de todas las categorias
        if categoria == "todos":
            sql = """
                SELECT YEAR(reviewTime) AS anio, count(review_id) AS total_reviews
                FROM reviews
                GROUP BY anio
                ORDER BY anio ASC;
            """
            cursor.execute(sql)

        # Si la categoria no es "todos" se muestra el numero de reviews por año de esa categoria
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
    
    titulo = f"Reviews por año de {categoria}" # Titulo del grafico

    # Se separan los datos en dos listas, una con los años y otra con el numero de reviews
    year = [dato[0] for dato in datos]
    total_reviews = [dato[1] for dato in datos]
    
    # Se muestra el grafico de barras con los años en el eje x y el numero de reviews en el eje y
    plt.figure(figsize=(10, 6))
    plt.bar(year, total_reviews)
    plt.title(titulo)
    plt.xlabel("Años")
    plt.ylabel("Numero de reviews")
    plt.xticks(year)
    plt.show()


def grafica_2():
    """
    Funcion que muestra un grafico de lineas con la evolucion de la popularidad de los articulos a lo largo del tiempo, se puede elegir una categoria o mostrar todas las categorias
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Se pide al usuario que elija una categoria
    categoria = elegir_categoria()

    with conexion:
        cursor = conexion.cursor()

        # Si la categoria es "todos" se muestra la evolucion de la popularidad de los articulos a lo largo del tiempo de todas las categorias
        if categoria == "todos":
            sql = """
                SELECT p.asin, p.tipo_producto, COUNT(r.review_id) AS total_reviews
                FROM reviews r
                    INNER JOIN productos p ON r.id_producto = p.id_producto
                GROUP BY p.asin, p.tipo_producto
                ORDER BY total_reviews DESC;
            """
            cursor.execute(sql)

        # Si la categoria no es "todos" se muestra la evolucion de la popularidad de los articulos a lo largo del tiempo de esa categoria
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

    titulo = f"Evolucion de popularidad de {categoria}" # Titulo del grafico
    
    # Se separa la popularidad de los articulos en una lista
    popularidad = [dato[2] for dato in datos]

    # Se crea una lista con el ranking de los articulos
    ranking = list(range(1, len(popularidad) + 1))

    # Se muestra el grafico de lineas con el ranking de los articulos en el eje x y la popularidad en el eje y
    plt.figure(figsize=(10, 6))
    plt.plot(ranking, popularidad)
    plt.title(titulo)
    plt.xlabel("Articulos")
    plt.ylabel("Numero de reviews")
    plt.show()


def posibles_cat_asin(asin):
    """
    Funcion que devuelve las posibles categorias de un articulo a partir de su asin
    """
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

    # Devuelve una lista con las categorias de ese asin, si no existe devuelve una lista vacia
    return [dato[1] for dato in datos]


def grafica_3():
    """
    Funcion que muestra un histograma con la distribucion de las reviews por nota, se puede elegir una categoria o mostrar todas las categorias, 
    o elegir un articulo concreto a partir de su asin
    """
    print(cf.OPCIONES_HISTOGRAMA)

    # Se pide al usuario que elija una opcion
    opcion = input("Opcion: ")

    # Se asegura de que la opcion introducida es valida
    while opcion not in cf.OPCIONES_HISTOGRAMA_POSIBLES:
        print("No es una opcion valida")
        opcion = input("Opcion (1, 2): ")

    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        # Si la opcion es 1 se muestra el histograma con la distribucion de las reviews por nota de una categoria o de todas las categorias
        if opcion == "1":
            categoria = elegir_categoria()

            # Si la categoria es "todos" se muestra el histograma con la distribucion de las reviews por nota de todas las categorias
            if categoria == "todos":
                sql = """
                    SELECT overall, COUNT(review_id) AS total_reviews
                    FROM reviews
                    GROUP BY overall
                    ORDER BY overall ASC;
                """
                cursor.execute(sql)

            # Si la categoria no es "todos" se muestra el histograma con la distribucion de las reviews por nota de esa categoria
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

            titulo = f"Reviews por nota de {categoria}" # Titulo del grafico de la opcion 1

        # Si la opcion es 2 se muestra el histograma con la distribucion de las reviews por nota de un articulo concreto a partir de su asin
        else:
            # Se pide al usuario que introduzca el asin del articulo y se adquieren las posibles categorias de ese asin
            asin = input("Introduce el asin del articulo: ")
            posibles_categorias = posibles_cat_asin(asin)

            # Si no existe ese asin se muestra un mensaje de error y se vuelve al menu
            if len(posibles_categorias) == 0:
                print("Ese asticulo no existe")
                return

            # Si solo existe una categoria para ese asin se muestra el histograma con la distribucion de las reviews por nota de ese articulo sin necesidad de elegir la categoria
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

                titulo = f"Histograma por nota del artículo {asin}" # Titulo del grafico de la opcion 2 cuando solo hay una categoria para ese asin

            # Si existen varias categorias para ese asin se pide al usuario que elija una categoria
            else:
                cat_validas = [i.replace("_", " ").lower() for i in posibles_categorias] # Se crea una lista con las categorias validas para ese asin

                # Se pide al usuario que elija una categoria de las categorias validas para ese asin
                categoria = input(f"Hay varios tipos de productos con ese asin, elige {cat_validas}: ").lower()

                # Se asegura de que la categoria introducida es una de las categorias validas para ese asin
                while categoria not in cat_validas:
                    print("Esa no es una de las categorias validas")
                    categoria = input(f"Hay varios tipos de productos con ese asin, elige {cat_validas}: ").lower()

                mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)

                # Se muestra el histograma con la distribucion de las reviews por nota de ese articulo y esa categoria
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
            
                titulo = f"Histograma por nota del artículo {asin} ({categoria})" # Titulo del grafico de la opcion 2 cuando hay varias categorias para ese asin

        datos = cursor.fetchall()

    # Se separa la nota de las reviews en una lista y el numero de reviews con esa nota en otra lista
    overall = [dato[0] for dato in datos]
    total_reviews = [dato[1] for dato in datos]

    # Se muestra el histograma con la nota de las reviews en el eje x y el numero de reviews con esa nota en el eje y
    plt.figure(figsize=(10, 6))
    plt.bar(overall, total_reviews)
    plt.title(titulo)
    plt.xlabel("Overall")
    plt.ylabel("Numero de reviews")
    plt.xticks(overall)
    plt.show()


def grafica_4():
    """
    Funcion que muestra un grafico de lineas con la evolucion de las reviews a lo largo del tiempo de todos los productos
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        # Se Consiguen los tipos de productos y el unixReviewTime de todas las reviews, ordenados por tipo de producto y por unixReviewTime
        sql = """
            SELECT p.tipo_producto, r.unixReviewTime
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            ORDER BY p.tipo_producto, unixReviewTime;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    # Se crea un diccionario con el numero de reviews por categoria y otro diccionario con las series para la grafica de lineas, se inicializan a 0 y a listas vacias
    reviews = {"Video_Games": 0, "Toys_and_Games": 0, "Digital_Music": 0, "Musical_Instruments": 0}
    series_grafica = {"Video_Games": [], "Toys_and_Games": [], "Digital_Music": [], "Musical_Instruments": []}

    # Para cada dato conseguido de SQL se suma 1 al numero de reviews de esa categoria y se añade una tupla con el unixReviewTime y el numero de reviews de esa categoria hasta es momento
    for categoria, unixtime in datos:
        reviews[categoria] += 1
        series_grafica[categoria].append((unixtime, reviews[categoria]))

    # Se muestra el grafico de lineas con el unixReviewTime en el eje x y el numero de reviews en el eje y, una linea para cada categoria
    plt.figure(figsize=(10, 6))
    for categoria, tupla in series_grafica.items(): #Para cada categoria se separa el unixReviewTime y el numero de reviews en dos listas y se muestra la linea de esa categoria
        x = [t[0] for t in tupla]
        y = [t[1] for t in tupla]
        plt.plot(x, y, label=categoria)
    plt.title("Evolución de las reviews a lo largo del tiempo de todos los productos")
    plt.xlabel("Tiempo")
    plt.ylabel("Numero de reviews hasta ese momento")
    plt.legend()
    plt.show()


def grafica_5():
    """
    Funcion que muestra un grafico de barras con el numero de usuarios por numero de reviews
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        # Se consiguen el reviewerID y el numero de reviews de cada usuario, ordenados por numero de reviews de forma descendente
        sql = """
            SELECT ReviewerID, COUNT(review_id) AS total_reviews
            FROM reviews
            GROUP BY ReviewerID;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    # Se crea una lista con el numero de reviews de cada usuario y se cuenta el numero de usuarios por numero de reviews con un Counter
    reviews_por_user = [dato[1] for dato in datos]
    numero_users_por_cant_reviews = Counter(reviews_por_user)

    # Se ordenan los datos por numero de reviews y se separa el numero de usuarios en una lista
    x = sorted(numero_users_por_cant_reviews.keys())
    y = [numero_users_por_cant_reviews[key] for key in x]

    # Se muestra el grafico de barras con el numero de reviews en el eje x y el numero de usuarios con ese numero de reviews en el eje y
    plt.figure(figsize=(10, 6))
    plt.bar(x, y)
    plt.title("Reviews por usuario")
    plt.xlabel("Numero de reviews")
    plt.ylabel("Numero de usuarios")
    plt.show()


def grafica_6():
    """
    Funcion que muestra una nube de palabras con los resumenes de las reviews, se puede elegir una categoria o mostrar todas las categorias
    """
    coleccion = conexion_mongodb()

    # Se pide al usuario que elija una categoria
    categoria = elegir_categoria()
    mapeo_categoria = cf.MAPEO_CATEGORIAS.get(categoria)
    titulo = f"Nube de palabras de {categoria}" # Titulo del grafico de la nube de palabras

    # Si la categoria es "todos" se encuentran los resumenes de todas las reviews
    if categoria == "todos":
        datos = coleccion.find({}, {"_id": 0, "summary": 1})
    
    # Si la categoria no es "todos" se encuentran los resumenes de las reviews de esa categoria
    else:
        datos = coleccion.find({"tipo_producto": mapeo_categoria}, {"_id": 0, "summary": 1})
    
    # Se crea una lista con los resumenes de las reviews y se concatenan en un solo string
    resumenes = [dato["summary"] for dato in datos]
    resumenes_concatenados = " ".join(resumenes)

    # Se separan las palabras del string concatenado y se eliminan las palabras de 3 o menos caracteres, luego se vuelven a concatenar en un solo string
    palabras = resumenes_concatenados.split(" ")
    palabras = [palabra for palabra in palabras if len(palabra) > 3]
    palabras_concatenadas = " ".join(palabras)
    
    # Se crea el objeto WordCloud con el string concatenado
    nube = WordCloud().generate(palabras_concatenadas)

    # Se muestra la nube de palabras
    plt.figure(figsize=(10, 6))
    plt.imshow(nube)
    plt.axis("off")
    plt.title(titulo)
    plt.show()


def grafica_7():
    """
    Funcion que muestra un grafico de barras con el promedio de overall por categoria
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        # Se consiguen el tipo de producto y el promedio de overall de cada categoria, ordenados por promedio de overall de forma ascendente
        sql = """
            SELECT p.tipo_producto, AVG(r.overall) AS media_overall
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            GROUP BY p.tipo_producto
            ORDER BY media_overall ASC;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    # Se crea una lista con las categorias y otra lista con el promedio de overall de cada categoria
    categorias = [dato[0] for dato in datos]
    media_overall = [dato[1] for dato in datos]

    # Se muestra el grafico de barras con las categorias en el eje x y el promedio de overall en el eje y
    plt.figure(figsize=(10, 6))
    bars = plt.bar(categorias, media_overall) # Se guarda el objeto de las barras para poder mostrar el valor de cada barra encima de ella
    plt.bar_label(bars, fmt="%.5f")
    plt.title("Promedio overall por categoria")
    plt.xlabel("Categoria")
    plt.ylabel("Promedio Overall")
    plt.show()


def main():
    """
    Funcion principal que muestra el menu de visualizacion y pide al usuario que elija una opcion, 
    se asegura de que la opcion introducida es valida y llama a la funcion correspondiente a la opcion elegida
    """
    print(cf.MENU)

    # Si la opcion es 8 se sale del menu, si no se muestra el menu de nuevo y se pide otra opcion
    opcion = 0
    while opcion != "8":
        # Se pide al usuario que elija una opcion
        opcion = input("Elige una opcion: ")

        # Se asegura de que la opcion introducida es valida
        while opcion not in cf.OPCIONES_MENU:
            print("Esa no es una opcion valida")
            opcion = input("Elige una opcion (1-8): ")

        # Dependiendo de la opcion elegida se llama a la funcion correspondiente
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