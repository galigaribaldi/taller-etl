from ETL.IngresosETL import IngresosETL
from ETL.NetflixETL import NetflixETL
from DB.conector_to_sqlite import base_multas, base_netflix
def netflix_etl():
    ###Creacion de la base
    #base_netflix()
    ###Carga de Excel a dataframe
    ETL_Netflix = NetflixETL(ruta="Charge", nombreArchivo="netflix_titles.xlsx")
    df = ETL_Netflix.extract_netflix()
    ###Generamos las listas
    listas = ETL_Netflix.generate_list_netflix(df)
    ###
    print("Tamaño de las listas", len(listas),)
    print("Estrctura de una lista: ", listas[0:2])
    ###Cargamos a la base
    ETL_Netflix.charge_netflix_batch(listas)
    ###Seleccionamos los datos
    res = ETL_Netflix.select_rows()
    print(res[0:1])
#netflix_etl()
def ingresos_etl():
    ###Creacion de la base
    base_multas()
    ###Carga de Excel a dataframe
    ETL_multas = IngresosETL(ruta="Charge", nombreArchivo="ingresos.xlsx")
    diccionarioDF = ETL_multas.extract_ingresos()
    print(diccionarioDF.keys())
    print(diccionarioDF[1][["ciclo", "periodo", "tipo","costo_total"]])
    #print(diccionarioDF[1])
    #print("\n\n")
    #print(diccionarioDF[8.1][["ciclo", "periodo", "tipo"]])
    ###Generamos las listas
    listas = ETL_multas.generate_list_ingresos(diccionarioDF[1])
    ###
    #print("Tamaño de las listas", len(listas),)
    print("Estrctura de una lista: ", listas[0:2])
    ###Cargamos a la base
    ETL_multas.charge_ingresos_batch(listas)
    ###Seleccionamos los datos
    res = ETL_multas.select_rows()
    print(res[0:1])
ingresos_etl()