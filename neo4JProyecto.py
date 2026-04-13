from neo4j import GraphDatabase
from load_data import conexion_mysql
import configuracion as cf
from menu_visualizacion import elegir_categoria


def conexion_neo4j():
    return GraphDatabase.driver(
        cf.NEO4J_URI,
        auth=(cf.NEO4J_USER, cf.NEO4J_PASSWORD)
    )


def limpiar_neo4j():
    driver = conexion_neo4j()
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()


# =========================
# APARTADO 4.1
# =========================

def top_usuarios():
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

    return [dato[0] for dato in datos]


def rating_y_medias():
    conexion = conexion_mysql(cf.MYSQL_DATABASE)

    usuarios = top_usuarios()
    if len(usuarios) == 0:
        return {}, {}

    usuarios_validos = ", ".join(["%s"] * len(usuarios))

    ratings_por_usuario = {user: {} for user in usuarios}
    sumas_overall = {user: 0.0 for user in usuarios}
    num_reviews = {user: 0 for user in usuarios}

    with conexion:
        cursor = conexion.cursor()
        sql = f"""
            SELECT r.reviewerID, p.asin, p.tipo_producto, r.overall
            FROM reviews r
                INNER JOIN productos p ON r.id_producto = p.id_producto
            WHERE r.reviewerID IN ({usuarios_validos});
        """
        cursor.execute(sql, tuple(usuarios))
        datos = cursor.fetchall()

    for reviewerID, asin, tipo_producto, overall in datos:
        item_id = (asin, tipo_producto)
        ratings_por_usuario[reviewerID][item_id] = float(overall)
        sumas_overall[reviewerID] += float(overall)
        num_reviews[reviewerID] += 1

    medias = {}
    for user in usuarios:
        if num_reviews[user] > 0:
            medias[user] = float(sumas_overall[user] / num_reviews[user])

    return ratings_por_usuario, medias


def correlacion(ratings_u, media_u, ratings_v, media_v):
    items_u_v = set(ratings_u.keys()) & set(ratings_v.keys())

    if len(items_u_v) == 0:
        return None

    numerador = 0.0
    suma_1 = 0.0
    suma_2 = 0.0

    for item in items_u_v:
        numerador += (ratings_u[item] - media_u) * (ratings_v[item] - media_v)
        suma_1 += (ratings_u[item] - media_u) ** 2
        suma_2 += (ratings_v[item] - media_v) ** 2

    if suma_1 == 0 or suma_2 == 0:
        return None

    return numerador / ((suma_1 ** 0.5) * (suma_2 ** 0.5))


def calcular_similitudes():
    usuarios = top_usuarios()
    ratings_por_usuario, medias = rating_y_medias()

    similitudes = {}

    for i in range(len(usuarios)):
        for j in range(i + 1, len(usuarios)):
            user_u = usuarios[i]
            user_v = usuarios[j]

            if user_u not in medias or user_v not in medias:
                continue

            correlacion_u_v = correlacion(
                ratings_por_usuario[user_u],
                medias[user_u],
                ratings_por_usuario[user_v],
                medias[user_v]
            )

            if correlacion_u_v is not None:
                key = (user_u, user_v)
                similitudes[key] = float(correlacion_u_v)

    return similitudes


def cargar_similitudes_neo4J():
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
            session.run(
                query,
                user_u=user_u,
                user_v=user_v,
                correlacion_u_v=correlacion_u_v
            )

    driver.close()


def usuario_mas_vecinos():
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

    if resultado is None:
        print("No hay usuarios cargados en Neo4j")
    else:
        print("Usuario con mas vecinos:", resultado["reviewerID"], "con", resultado["vecinos"], "vecinos")


def opcion_1():
    limpiar_neo4j()
    cargar_similitudes_neo4J()
    usuario_mas_vecinos()


# =========================
# APARTADO 4.2
# =========================

def pedir_num_articulos():
    entero = False

    while not entero:
        try:
            num_articulos = int(input("Elige una cantidad de articulos: "))
            if num_articulos <= 0:
                print("La cantidad tiene que ser mayor que 0")
            else:
                entero = True
        except ValueError:
            print("El numero introducido no es entero")

    return num_articulos


def obtener_articulos_aleatorios():
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