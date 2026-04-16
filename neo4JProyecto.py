from neo4j import GraphDatabase
from load_data import conexion_mysql
import configuracion as cf
from menu_visualizacion import elegir_categoria




def conexion_neo4j():
    """
    Función para establecer la conexión con Neo4j utilizando los parámetros de configuración
    """
    return GraphDatabase.driver(cf.NEO4J_URI, auth=(cf.NEO4J_USER, cf.NEO4J_PASSWORD))


def limpiar_neo4j():
    """
    Funcion para eliminar todos los nodos y relaciones de la base de datos Neo4j, dejando la base de datos vacía
    """
    driver = conexion_neo4j()

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    driver.close()


# 4.1

def top_usuarios():
    """
    Funcion para obtener los usuarios con más reviews ordenados por número de reviews y reviewerID
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()

        sql = """
            SELECT reviewerID
            FROM reviews
            GROUP BY reviewerID
            ORDER BY COUNT(review_id) DESC
            LIMIT %s;
        """
        cursor.execute(sql, (cf.TOP_USUARIOS_REVIEWS,))

        datos = cursor.fetchall()

    # Devolvemos solo los reviewerID en una lista
    return [dato[0] for dato in datos]


def rating_y_medias():
    """
    Funcion para obtener los ratings de cada usuario por cada artículo y la media de ratings de cada usuario, 
    utilizando solo los usuarios obtenidos en top_usuarios
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Si no hay usuarios, devolvemos diccionarios vacíos para evitar errores posteriores
    usuarios = top_usuarios()
    if len(usuarios) == 0:
        return {}, {}

    # Creamos una cadena de placeholders para la consulta SQL, con tantos %s como usuarios haya
    usuarios_validos = ", ".join(["%s"] * len(usuarios))

    # Inicializamos los diccionarios para almacenar los ratings por usuario, las sumas de ratings y el número de reviews
    ratings_por_usuario = {user: {} for user in usuarios}
    sumas_overall = {user: 0.0 for user in usuarios}
    num_reviews = {user: 0 for user in usuarios}

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT r.reviewerID, p.id_producto, r.overall
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE r.reviewerID IN ({usuarios_validos});
        """
        cursor.execute(sql, tuple(usuarios))

        datos = cursor.fetchall()

    # Recorremos los datos obtenidos de SQL para llenar los diccionarios
    for reviewerID, id_producto, overall in datos:
        ratings_por_usuario[reviewerID][id_producto] = float(overall) # Guardamos el rating del usuario para ese producto
        sumas_overall[reviewerID] += float(overall) # Sumamos el rating al total de ratings del usuario para luego calcular la media
        num_reviews[reviewerID] += 1 # Contamos el número de reviews del usuario para luego calcular la media

    # Inicializamos el diccionario de medias
    medias = {}
    for user in usuarios:
        # Para cada usuario si ha hecho algun review (para evitar división por cero) calculamos su media de overall
        if num_reviews[user] > 0:
            medias[user] = float(sumas_overall[user] / num_reviews[user])

    # Devolvemos el diccionario de ratings por usuario y el diccionario de medias por usuario
    return ratings_por_usuario, medias


def correlacion(ratings_u, media_u, ratings_v, media_v):
    """
    Funcion para calcular la correlacion entre dos usuarios u y v, utilizando sus ratings y medias de ratings.
    """
    # Items que ambos usuarios han valorado en comun
    items_u_v = set(ratings_u.keys()) & set(ratings_v.keys())

    # Si no hay items en comun, no se puede calcular la correlacion, devolvemos None
    if len(items_u_v) == 0:
        return None

    # Inicializamos las variables para calcular el numerador y los denominadores de la formula de correlacion
    numerador = 0.0
    suma_1 = 0.0
    suma_2 = 0.0

    # Para cada item valorado por ambos usuarios, sumamos lo debido, dicho por la formula del PDF
    for item in items_u_v:
        numerador += (ratings_u[item] - media_u) * (ratings_v[item] - media_v)
        suma_1 += (ratings_u[item] - media_u) ** 2
        suma_2 += (ratings_v[item] - media_v) ** 2

    # Evitamos division por cero y devolvemos None
    if suma_1 == 0 or suma_2 == 0:
        return None

    # Completamos la formula de correlacion y devolvemos el resultado
    return numerador / ((suma_1 ** 0.5) * (suma_2 ** 0.5))


def calcular_similitudes():
    """
    Formula para calcular las similitudes entre los usuarios obtenidos en top_usuarios
    """
    usuarios = top_usuarios() # Obtenemos los usuarios con más reviews
    ratings_por_usuario, medias = rating_y_medias() # Obtenemos los ratings por usuario y las medias de ratings por usuario

    # Inicializamos el diccionario de similitudes
    similitudes = {}

    # Recorremos todos los pares de usuarios sin repetir ningun par
    for i in range(len(usuarios)):
        for j in range(i + 1, len(usuarios)):
            user_u = usuarios[i]
            user_v = usuarios[j]

            # Si alguno de los usuarios no tiene media calculada (lo que significa que no tiene reviews), no se calcula nada
            if user_u not in medias or user_v not in medias:
                continue
            
            # Calculamos la correlacion entre los dos usuarios 
            correlacion_u_v = correlacion(ratings_por_usuario[user_u], medias[user_u], ratings_por_usuario[user_v], medias[user_v])

            # Si hay correlacion guardamos el par con el valor de la correlacion en el diccionario de similitudes
            if correlacion_u_v is not None:
                key = (user_u, user_v)
                similitudes[key] = float(correlacion_u_v)

    # Devolvemos el diccionario de similitudes entre los usuarios
    return similitudes


def cargar_similitudes_neo4J():
    """
    Funcion para cargar en Neo4j los usuarios que tienen alguna similitudcalculada entre ellos
    """
    driver = conexion_neo4j()
    similitudes = calcular_similitudes()

    with driver.session() as session:
        for (user_u, user_v), correlacion_u_v in similitudes.items():
            query = """
                MERGE (u:Usuario {reviewerID: $user_u})
                MERGE (v:Usuario {reviewerID: $user_v})
                MERGE (u)-[r:SIMILITUD]-(v)
                SET r.correlacion = $correlacion_u_v
            """
            session.run(query, user_u=user_u, user_v=user_v, correlacion_u_v=correlacion_u_v)

    driver.close()


def usuario_mas_vecinos():
    """
    Funcion para obtener el usuario con más vecinos en Neo4j, el usuario que tiene más relaciones de tipo SIMILITUD
    """
    driver = conexion_neo4j()

    with driver.session() as session:
        query = """
            MATCH (u:Usuario)-[:SIMILITUD]-(:Usuario)
            RETURN u.reviewerID AS reviewerID,
                   COUNT(*) AS vecinos
            ORDER BY vecinos DESC, reviewerID ASC
            LIMIT 1
        """
        resultado = session.run(query).single()

    driver.close()

    # Si no hay resultado, no hay usuarios cargados en Neo4j, si hay, lo mostramos por terminal
    if resultado is None:
        print("No hay usuarios cargados en Neo4j")
    else:
        print("Usuario con mas vecinos:", resultado["reviewerID"], "con", resultado["vecinos"], "vecinos")


def opcion_1():
    limpiar_neo4j()
    cargar_similitudes_neo4J()
    usuario_mas_vecinos()


# 4.2

def pedir_num_articulos():
    """
    Funcion para pedir al usuario un número entero de artículos, validando que el número introducido sea un entero positivo
    """
    # Se asume que no es entero para entrar en el bucle
    entero = False

    # Mientras no sea entero
    while not entero:
        # intetentamos convertir la entrada a entero, si no se puede, se muestra un mensaje de error y se vuelve a pedir el número
        try:
            num_articulos = int(input("Elige una cantidad de articulos: "))
            if num_articulos <= 0:
                print("La cantidad tiene que ser mayor que 0")
            else:
                entero = True
        except ValueError:
            print("El numero introducido no es entero")

    # Devolvemos el entero
    return num_articulos


def obtener_articulos_aleatorios():
    """
    Funcion que obtiene una cantidad de artículos aleatorios de una categoría elegida por el usuario, validando que la cantidad de artículos solicitada no sea mayor que la cantidad de artículos disponibles en esa categoría, y devolviendo los ASINs de los artículos obtenidos y el tipo de producto correspondiente a la categoría elegida
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Le pedimos al usuario que elija una categoría, que no sea "todos" ya que no se permite esa opción en esta parte
    categoria = elegir_categoria()
    while categoria == "todos":
        print("En este apartado no se permite elegir 'todos'")
        categoria = elegir_categoria()

    # Obtenemos el tipo de articulo tal y como esta en la base de datos
    mapeo_categoria = cf.MAPEO_CATEGORIAS[categoria]

    with conexion:
        cursor = conexion.cursor()

        # Comprobamos que la cantidad de artículos solicitada no sea mayor que la cantidad de artículos disponibles en esa categoría
        cantidad_posible = False

        while not cantidad_posible:
            num_articulos = pedir_num_articulos()

            sql = """
                SELECT asin
                FROM productos
                WHERE tipo_producto = %s
                ORDER BY RAND()
                LIMIT %s;
            """
            cursor.execute(sql, (mapeo_categoria, num_articulos))

            datos = cursor.fetchall()
            
            # Chequeamos si la cantidad de artículos obtenida es menor que la cantidad solicitada, si es asi se repite pidiendo otra cantidad de articulos
            if len(datos) < num_articulos:
                print("No hay tantos articulos en", categoria, "solo hay", len(datos))
            # Si la cantidad es correcta, seguimos con esos datos
            else:
                cantidad_posible = True

        asins = [dato[0] for dato in datos]

    # Devolvemos los articulos y la categoria elegida
    return asins, mapeo_categoria


def reviews_por_articulo():
    """
    Funcion que obtiene las reviews de dichos articulos aleatorios
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Obtenemos los asins, y el tipo de producto de los artículos aleatorios obtenidos
    asins, mapeo_categoria = obtener_articulos_aleatorios()
    asins_validos = ", ".join(["%s"] * len(asins)) # Creamos la lista de placeholders de longitud de asins
    parametros = [mapeo_categoria] + asins # Creamos una lista de parametros para la consulta AQL

    # Creamos un diccionario para cada asin, con valor diccionario donde se guardan las reviews de cada usuario
    reviews_asin = {asin: {} for asin in asins}

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT p.asin, r.reviewerID, r.reviewTime, r.overall
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE p.tipo_producto = %s
                AND p.asin IN ({asins_validos});
        """
        cursor.execute(sql, tuple(parametros))

        datos = cursor.fetchall()

    # Para cada dato obtenido de la consulta SQL
    for asin, reviewerID, reviewTime, overall in datos:
        # Para el asin y el reviewerID, guardamos el reviewTime y el overall en formato tupla
        reviews_asin[asin][reviewerID] = (reviewTime, overall)

    # Devolvemos el diccionario de reviews por asin y el tipo de producto de la categoria elegida
    return reviews_asin, mapeo_categoria


def cargar_reviews_por_articulo_neo4J():
    """
    Funcion para cargar en Neo4j las relaciones entre los usuarios y articulos obtenidos
    """
    driver = conexion_neo4j()
    # Obtenemos las reviews por asin y el tipo de producto de la categoria elegida
    reviews_asin, mapeo_categoria = reviews_por_articulo()

    with driver.session() as session:
        # Para cada asin y el diccionario valor de los reviews
        for asin, reviews in reviews_asin.items():
            # Para el reviewerID y la tupla de reviewTime y overall de cada review
            for reviewerID, (reviewTime, overall) in reviews.items():
                query = """
                    MERGE (a:Articulo {asin: $asin, tipo_producto: $tipo_producto})
                    MERGE (u:Usuario {reviewerID: $reviewerID})
                    MERGE (u)-[r:REVIEWED]->(a)
                    SET r.reviewTime = $reviewTime,
                        r.overall = $overall
                """
                session.run(query, asin=asin, tipo_producto=mapeo_categoria, reviewerID=reviewerID, reviewTime=str(reviewTime), overall=float(overall))

    driver.close()


def opcion_2():
    limpiar_neo4j()
    cargar_reviews_por_articulo_neo4J()
    print("Se ha terminado la carga de datos en Neo4j")


# 4.3

def primeros_usuarios():
    """
    Funcion para obtener los reviewerID de los primeros usuarios ordenados por reviewerName
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            SELECT reviewerID
            FROM usuarios
            ORDER BY reviewerName ASC
            LIMIT %s;
        """
        cursor.execute(sql, (cf.LIMIT_USUARIOS_ALEATORIOS,))
        datos = cursor.fetchall()

    # Devolvemos solo los reviewerID en una lista
    return [dato[0] for dato in datos]


def obtener_reviews_primeros_usuarios():
    """
    Funcion para obtener el número de artículos distintos que han revisado los primeros usuarios obtenidos y para que tipo de producto
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Obtenemos los reviewerID de los primeros usuarios ordenados por reviewerName, si no hay usuarios devolvemos un diccionario vacío
    usuarios = primeros_usuarios()
    if len(usuarios) == 0:
        return {}

    # Creamos una cadena de placeholders para la consulta SQL con la longitud de usuarios obtenidos
    usuarios_validos = ", ".join(["%s"] * len(usuarios))

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT r.reviewerID, p.tipo_producto, COUNT(DISTINCT p.asin) AS num_articulos
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE r.reviewerID IN ({usuarios_validos})
            GROUP BY r.reviewerID, p.tipo_producto;
        """
        cursor.execute(sql, tuple(usuarios))

        datos = cursor.fetchall()

    # Creamos un diccionario de las reviews
    reviews = {}

    # Para cada reviewerID, tipo_producto y num_articulos obtenido de la consulta SQL
    for reviewerID, tipo_producto, num_articulos in datos:
        # Si el reviewerID no esta en el diccionario de reviews, lo añadimos con un diccionario vacío como valor
        if reviewerID not in reviews:
            reviews[reviewerID] = {}
        # Para el reviewerID y el tipo_producto, guardamos el número de artículos distintos
        reviews[reviewerID][tipo_producto] = int(num_articulos)

    # Devolvemos el diccionario de reviews por usuario y tipo de producto
    return reviews


def cargar_primeros_usuarios_neo4J():
    """
    Funcion para cargar en Neo4j los primeros usuarios obtenidos y las relaciones de reviews con el número de artículos para cada tipo de producto
    """
    driver = conexion_neo4j()
    # Obtenemos el diccionario de reviews por usuario y tipo de producto de los primeros usuarios obtenidos
    reviews = obtener_reviews_primeros_usuarios()

    with driver.session() as session:
        # Para cada reviewerID y el diccionario de tipo_producto y num_articulos
        for reviewerID, tipo_cantidad in reviews.items():
            # Si el usuaruio ha revisado al menos 2 tipos de productos
            if len(tipo_cantidad) >= 2:
                # Para cada tipo de roducto y cantidad de artículos revisados por el usuario
                for tipo_producto, cantidad in tipo_cantidad.items():
                    query = """
                        MERGE (u:Usuario {reviewerID: $reviewerID})
                        MERGE (t:TipoProducto {nombre: $tipo_producto})
                        MERGE (u)-[r:CONSUME]->(t)
                        SET r.num_articulos = $cantidad
                    """
                    session.run(query, reviewerID=reviewerID, tipo_producto=tipo_producto, cantidad=int(cantidad))

    driver.close()


def opcion_3():
    limpiar_neo4j()
    cargar_primeros_usuarios_neo4J()
    print("Se ha terminado la carga de datos en Neo4j")


# 4.4

def obtener_top_articulos():
    """
    Funcion para obtener los 5 artículos con menos de 40 reseñas
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            SELECT p.id_producto, COUNT(r.review_id) AS total_reviews
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            GROUP BY p.id_producto
            HAVING total_reviews < 40
            ORDER BY total_reviews DESC, p.id_producto ASC
            LIMIT 5;
        """
        cursor.execute(sql)

        datos = cursor.fetchall()

    # Devolvemos los id_producto en una lista
    return [dato[0] for dato in datos]


def obtener_usuarios_articulos():
    """
    Funcion para obtener los usuarios que han revisado los artículos obtenidos
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Si no hay artículos, devolvemos un diccionario vacío para evitar errores mas tarde
    articulos = obtener_top_articulos()
    if len(articulos) == 0:
        return {}

    # Creamos los placeholders para la consulta SQL
    articulos_validos = ", ".join(["%s"] * len(articulos))

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT p.id_producto, r.reviewerID
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE p.id_producto IN ({articulos_validos});
        """
        cursor.execute(sql, tuple(articulos))

        datos = cursor.fetchall()

    # Creamos un diccionario reviews donde para cada producto la clave son los usuarios que lo han revisado
    reviews = {}

    # Para cada id_producto y reviewerID obtenido de la consulta SQL
    for id_producto, reviewerID in datos:
        # Si el id_producto no esta en el diccionario de reviews lo añadimos al set
        if id_producto not in reviews:
            reviews[id_producto] = {}
        reviews[id_producto].add(reviewerID)

    # Devolvemos el diccionario de reviews 
    return reviews


def articulos_en_comun():
    """
    Funcion que calcula el numero de articulos en comun que han revisado los usuarios que han revisado los artículos obtenidos
    """
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    # Se obtiene el diccionario reviews, si no hay reviews, devolvemos un diccionario vacío para evitar errores posteriores
    reviews = obtener_usuarios_articulos()
    if len(reviews) == 0:
        return {}

    # Creamos un set de usuarios
    users = {}
    #Para cada lista de usuarios que han revisado un producto
    for lista_users in reviews.values():
        users.update(lista_users) # Añadimos los usuarios no duplicados al set

    # Creamos los placeholders para la consulta SQL con la longitud de usuarios obtenidos
    users = list(users)
    users_validos = ", ".join(["%s"] * len(users))

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT r.reviewerID, p.id_producto
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE r.reviewerID IN ({users_validos});
        """
        cursor.execute(sql, tuple(users))

        datos = cursor.fetchall()

    # Creamos un diccionario de articulos por usuario
    articulos_por_user = {}
    # Para cada reviewerID y id_producto obtenido de la consulta SQL
    for reviewerID, id_producto in datos:
        # Si el reviewerID no esta en el diccionario de articulos por usuario, lo añadimos con un set vacío como valor
        if reviewerID not in articulos_por_user:
            articulos_por_user[reviewerID] = {}
        articulos_por_user[reviewerID].add(id_producto) # Añadimos el id_producto al set de articulos revisados por el usuario

    # Creamos un diccionario de artículos en común entre usuarios
    comunes = {}
    usuarios = [key for key in articulos_por_user.keys()] # Lista de usuarios 

    # Recorremos todos los pares de usuarios sin repetir ningun par
    for i in range(len(usuarios)):
        for j in range(i + 1, len(usuarios)):
            user_u = usuarios[i]
            user_v = usuarios[j]

            # Calculamos los articulos en comun entre los dos usuarios
            articulos_u_v = articulos_por_user[user_u] & articulos_por_user[user_v]

            # Calculamos el numero de articulos en comun si hay alguno, y los guardamos en el diccionario de comunes para el par de usuarios
            if len(articulos_u_v) > 0:
                key = (user_u, user_v)
                comunes[key] = len(articulos_u_v)

    # Devolvemos el diccionario de artículos en común entre usuarios
    return comunes


def cargar_top_articulos_usuarios_neo4J():
    """
    Funcion que carga las relaciones entre pares de usuarios y relacion entre producto y usuario
    """
    driver = conexion_neo4j()

    # Obtenemos los diccionarios de reviews por producto y el número de artículos en común entre usuarios
    reviews = obtener_usuarios_articulos()
    comunes = articulos_en_comun()

    with driver.session() as session:
        # Para cada producto y el set de usuarios que lo han revisado
        for id_producto, usuarios in reviews.items():
            # Para cada usuario del set
            for reviewerID in usuarios:
                query = """
                    MERGE (u:Usuario {reviewerID: $reviewerID})
                    MERGE (a:Articulo {id_producto: $id_producto})
                    MERGE (u)-[:REVIEWS]->(a)
                """
                session.run(query, reviewerID=reviewerID, id_producto=int(id_producto))

        # PAra cada par de usuarios y el numero de articulos en comun entre ellos
        for (user_u, user_v), num_comun in comunes.items():
            query = """
                MERGE (u:Usuario {reviewerID: $user_u})
                MERGE (v:Usuario {reviewerID: $user_v})
                MERGE (u)-[r:COMUN]-(v)
                SET r.num_articulos_comun = $cantidad
            """
            session.run(query, user_u=user_u, user_v=user_v, cantidad=int(num_comun))

    driver.close()


def opcion_4():
    limpiar_neo4j()
    cargar_top_articulos_usuarios_neo4J()
    print("Se ha terminado la carga de datos en Neo4j")


def main():
    """
    Funcion principal que muestra el menu en cada iteracion y permite elegir la opcion
    """
    opcion = "" # Asegura que entremos en el bucle del menu

    # Mientras no se elija la opcion 5
    while opcion != "5":
        print(cf.MENU_NEO4J)

        # Pedimos que se elija una opcion, y validamos que la opcion introducida sea una opcion valida del menu
        opcion = input("Elige una opcion: ")
        while opcion not in cf.OPCIONES_VALIDAS_NEO4J:
            print("Esa no es una opcion valida")
            opcion = input("Elige una opcion (1-5): ")

        # Para cada opcion, se llama a la funcion correspondiente para realizar la tarea indicada en el menu

        if opcion == "1":
            opcion_1()

        elif opcion == "2":
            opcion_2()

        elif opcion == "3":
            opcion_3()

        elif opcion == "4":
            opcion_4()




if __name__ == "__main__":
    main()