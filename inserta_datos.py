from load_data import inserta_datos_mysql, inserta_mongodb
import json
from datetime import datetime
import configuracion as cf




def main():
    inserta_datos_mysql(cf.SPORTS_AND_OUTDOORS)
    inserta_mongodb(cf.SPORTS_AND_OUTDOORS)




if __name__ == "__main__":
    main()