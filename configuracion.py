USER = "miguel"
PASSWORD = "alumno.IMAT2026"
MYSQL_HOST = "localhost"
MYSQL_DATABASE = "proyecto_reviews_sql"

CONNECTION_STRING = "mongodb://localhost:27017/"
MONGO_DATABASE = "proyecto_reviews_mongo"
MONGO_COLLECTION = "reviews_texto"

VIDEO_GAMES = r"data\Video_Games_5.json"
TOY_AND_GAMES = r"data\Toys_and_Games_5.json"
DIGITAL_MUSIC = r"data\Digital_Music_5.json"
MUSICAL_INSTRUMENTS = r"data\Musical_Instruments_5.json"

RUTAS = [VIDEO_GAMES, TOY_AND_GAMES, DIGITAL_MUSIC, MUSICAL_INSTRUMENTS]

MAX_LOTE = 1000

SQL_TABLA_USUARIOS = """
                    CREATE TABLE IF NOT EXISTS usuarios (
                        reviewerID VARCHAR(30) PRIMARY KEY,
                        reviewerName VARCHAR(255)
                    );"""
  
SQL_TABLA_PRODUCTOS = """
                    CREATE TABLE IF NOT EXISTS productos (
                        id_producto INT AUTO_INCREMENT PRIMARY KEY,
                        asin VARCHAR(20),
                        tipo_producto VARCHAR(50),
                        UNIQUE (asin, tipo_producto)
                    );"""

SQL_TABLA_REVIEWS = """
                    CREATE TABLE IF NOT EXISTS reviews (
                        review_id INT AUTO_INCREMENT PRIMARY KEY,
                        reviewerID VARCHAR(30),
                        id_producto INT,
                        overall FLOAT,
                        unixReviewTime BIGINT,
                        reviewTime DATE,
                        helpful_1 INT,
                        helpful_2 INT,
                        FOREIGN KEY (reviewerID) REFERENCES usuarios(reviewerID),
                        FOREIGN KEY (id_producto) REFERENCES productos(id_producto)
                    );"""

MENU = """
========== MENU ==========
1. Mostrar la evolución de reviews por años
2. Evolucion de la popularidad de los articulos
3. Histograma por nota
4. Evolucion de las reviews a lo largo del tiempo para todas las categorias
5. Histograma de reviews por usuario
6. Nube de palabras por categoria
7. Box plot de notas por categoria
8. Salir
"""
OPCIONES_MENU = [f"{i}" for i in range(1, 9)]

CATEGORIAS_VALIDAS = ["video games", "toys and games", "digital music", "musical instruments", "todos"]
MAPEO_CATEGORIAS = {"video games": "Video_Games", "toys and games": "Toys_and_Games", "digital music": "Digital_Music", "musical instruments": "Musical_Instruments"}

OPCIONES_HISTOGRAMA = """
1. Por catoegoria o por todos
2. Por articulo individual
"""
OPCIONES_HISTOGRAMA_POSIBLES = ["1", "2"]

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "donkikong"

TOP_USUARIOS_REVIEWS = 30
LIMIT_USUARIOS_ALEATORIOS = 400

MENU_NEO4J = """
========== MENU NEO4J ==========
1. Similitudes entre usuarios
2. Enlaces entre usuarios y articulos aleatorios
3. Usuarios que han puntuado mas de un tipo de articulo
4. Articulos populares y articulos en comun entre usuarios
5. Salir
"""
OPCIONES_VALIDAS_NEO4J = ["1", "2", "3", "4", "5"]

SPORTS_AND_OUTDOORS = r"data\Sports_and_Outdoors_5.json"