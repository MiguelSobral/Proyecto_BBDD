from load_data import inserta_datos_mysql, inserta_mongodb
import configuracion as cf




def main():
    """
    Funcion que carga datos de otro JSON a SQL y mongo
    """
    # Llmamos a las funciones de carga de datos
    inserta_datos_mysql(cf.SPORTS_AND_OUTDOORS)
    inserta_mongodb(cf.SPORTS_AND_OUTDOORS)




if __name__ == "__main__":
    main()