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

    categoria = elegir_categoria()
    while categoria == "todos":
        print("En este apartado no se permite elegir 'todos'")
        categoria = elegir_categoria()

    mapeo_categoria = cf.MAPEO_CATEGORIAS[categoria]

    with conexion:
        cursor = conexion.cursor()

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

            if len(datos) < num_articulos:
                print("No hay tantos articulos en", categoria, "solo hay", len(datos))
            else:
                cantidad_posible = True

        asins = [dato[0] for dato in datos]

    return asins, mapeo_categoria


def reviews_por_articulo():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    asins, mapeo_categoria = obtener_articulos_aleatorios()
    asins_validos = ", ".join(["%s"] * len(asins))
    parametros = [mapeo_categoria] + asins

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

    for asin, reviewerID, reviewTime, overall in datos:
        reviews_asin[asin][reviewerID] = (reviewTime, overall)

    return reviews_asin, mapeo_categoria


def cargar_reviews_por_articulo_neo4J():
    driver = conexion_neo4j()
    reviews_asin, mapeo_categoria = reviews_por_articulo()

    with driver.session() as session:
        for asin, reviews in reviews_asin.items():
            for reviewerID, (reviewTime, overall) in reviews.items():
                query = """
                    MERGE (a:Articulo {asin: $asin, tipo_producto: $tipo_producto})
                    MERGE (u:Usuario {reviewerID: $reviewerID})
                    MERGE (u)-[r:REVIEWED]->(a)
                    SET r.reviewTime = $reviewTime,
                        r.overall = $overall
                """
                session.run(
                    query,
                    asin=asin,
                    tipo_producto=mapeo_categoria,
                    reviewerID=reviewerID,
                    reviewTime=str(reviewTime),
                    overall=float(overall)
                )

    driver.close()


def opcion_2():
    limpiar_neo4j()
    cargar_reviews_por_articulo_neo4J()
    print("Se ha terminado la carga de datos en Neo4j")


# =========================
# APARTADO 4.3
# =========================

def primeros_usuarios():
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

    return [dato[0] for dato in datos]


def obtener_reviews_primeros_usuarios():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    usuarios = primeros_usuarios()
    if len(usuarios) == 0:
        return {}

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

    reviews = {}

    for reviewerID, tipo_producto, num_articulos in datos:
        if reviewerID not in reviews:
            reviews[reviewerID] = {}
        reviews[reviewerID][tipo_producto] = int(num_articulos)

    return reviews


def cargar_primeros_usuarios_neo4J():
    driver = conexion_neo4j()
    reviews = obtener_reviews_primeros_usuarios()

    with driver.session() as session:
        for reviewerID, tipo_cantidad in reviews.items():
            if len(tipo_cantidad) >= 2:
                for tipo_producto, cantidad in tipo_cantidad.items():
                    query = """
                        MERGE (u:Usuario {reviewerID: $reviewerID})
                        MERGE (t:TipoProducto {nombre: $tipo_producto})
                        MERGE (u)-[r:CONSUME]->(t)
                        SET r.num_articulos = $cantidad
                    """
                    session.run(
                        query,
                        reviewerID=reviewerID,
                        tipo_producto=tipo_producto,
                        cantidad=int(cantidad)
                    )

    driver.close()


def opcion_3():
    limpiar_neo4j()
    cargar_primeros_usuarios_neo4J()
    print("Se ha terminado la carga de datos en Neo4j")


# =========================
# APARTADO 4.4
# =========================

def obtener_top_articulos():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    with conexion:
        cursor = conexion.cursor()
        sql = """
            SELECT p.asin, p.tipo_producto, COUNT(r.review_id) AS total_reviews
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            GROUP BY p.asin, p.tipo_producto
            HAVING total_reviews < 40
            ORDER BY total_reviews DESC, p.asin ASC
            LIMIT 5;
        """
        cursor.execute(sql)
        datos = cursor.fetchall()

    return [(dato[0], dato[1]) for dato in datos]


def obtener_usuarios_articulos():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    articulos = obtener_top_articulos()
    if len(articulos) == 0:
        return {}

    condicion_valida = " OR ".join(["(p.asin = %s AND p.tipo_producto = %s)"] * len(articulos))

    parametros = []
    for asin, tipo_producto in articulos:
        parametros.append(asin)
        parametros.append(tipo_producto)

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT p.asin, p.tipo_producto, r.reviewerID
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE {condicion_valida};
        """
        cursor.execute(sql, tuple(parametros))
        datos = cursor.fetchall()

    reviews = {}

    for asin, tipo_producto, reviewerID in datos:
        key = (asin, tipo_producto)
        if key not in reviews:
            reviews[key] = set()
        reviews[key].add(reviewerID)

    return reviews


def articulos_en_comun():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    reviews = obtener_usuarios_articulos()
    if len(reviews) == 0:
        return {}

    users = set()
    for lista_users in reviews.values():
        users.update(lista_users)

    users = list(users)
    users_validos = ", ".join(["%s"] * len(users))

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT r.reviewerID, p.asin, p.tipo_producto
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE r.reviewerID IN ({users_validos});
        """
        cursor.execute(sql, tuple(users))
        datos = cursor.fetchall()

    articulos_por_user = {}
    for reviewerID, asin, tipo_producto in datos:
        articulo = (asin, tipo_producto)
        if reviewerID not in articulos_por_user:
            articulos_por_user[reviewerID] = set()
        articulos_por_user[reviewerID].add(articulo)

    comunes = {}
    usuarios = list(articulos_por_user.keys())

    for i in range(len(usuarios)):
        for j in range(i + 1, len(usuarios)):
            user_u = usuarios[i]
            user_v = usuarios[j]

            asins_u_v = articulos_por_user[user_u] & articulos_por_user[user_v]

            if len(asins_u_v) > 0:
                key = (user_u, user_v)
                comunes[key] = len(asins_u_v)

    return comunes


def cargar_top_articulos_usuarios_neo4J():
    driver = conexion_neo4j()

    reviews = obtener_usuarios_articulos()
    comunes = articulos_en_comun()

    with driver.session() as session:
        for (asin, tipo_producto), usuarios in reviews.items():
            for reviewerID in usuarios:
                query = """
                    MERGE (u:Usuario {reviewerID: $reviewerID})
                    MERGE (a:Articulo {asin: $asin, tipo_producto: $tipo_producto})
                    MERGE (u)-[:REVIEWS]->(a)
                """
                session.run(
                    query,
                    reviewerID=reviewerID,
                    asin=asin,
                    tipo_producto=tipo_producto
                )

        for (user_u, user_v), num_comun in comunes.items():
            query = """
                MERGE (u:Usuario {reviewerID: $user_u})
                MERGE (v:Usuario {reviewerID: $user_v})
                MERGE (u)-[r:COMUN]-(v)
                SET r.num_articulos_comun = $cantidad
            """
            session.run(
                query,
                user_u=user_u,
                user_v=user_v,
                cantidad=int(num_comun)
            )

    driver.close()


def opcion_4():
    limpiar_neo4j()
    cargar_top_articulos_usuarios_neo4J()
    print("Se ha terminado la carga de datos en Neo4j")


# =========================
# MAIN
# =========================

def main():
    opcion = ""

    while opcion != "5":
        print(cf.MENU_NEO4J)

        opcion = input("Elige una opcion: ")
        while opcion not in cf.OPCIONES_VALIDAS_NEO4J:
            print("Esa no es una opcion valida")
            opcion = input("Elige una opcion (1-5): ")

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