import pandas as pd
import sqlite3 
class NetflixETL():
    
    ruta = "Charge"
    nombreArchivo=""
    rutaCompleta = ""
    
    def __init__(self, ruta, nombreArchivo):
        self.ruta = ruta
        self.nombreArchivo = nombreArchivo
        self.rutaCompleta = self.ruta + "/"+self.nombreArchivo
        
    
    def extract_netflix(self):
        archivo = pd.read_excel(self.rutaCompleta)
        print("Las columnas del archivo: ", archivo.columns)
        print("El tamaño del archivo: ", len(archivo))
        return archivo    
    
    def generate_list_netflix(self, dataframe):
        lista = list(tuple([
            int(i),
            str(dataframe['title'][i]),
            str(dataframe['director'][i]),
            str(dataframe['cast'][i]),
            str(dataframe['rating'][i]),
            str(dataframe['duration'][i]),
            str(dataframe['description'][i])
            ])for i in range(len(dataframe)))
        return lista
    
    def charge_netflix_batch(self, lists,ruta=True):
        if ruta == False:
            con = sqlite3.connect("BasePrueba.db")
        else:
            con = sqlite3.connect("DB/BasePrueba.db")
            
        query = "INSERT INTO catalogo_netflix(catalogo_netflix,title,director, cast, rating, duration,descripcion) VALUES (?,?,?,?,?,?,?)"        
        cursor = con.cursor()
        cursor.executemany(query, lists)
        cursor.close()
        con.commit()  
        
    def select_rows(self, ruta=True):
        if ruta == False:
            con = sqlite3.connect("BasePrueba.db")
        else:
            con = sqlite3.connect("DB/BasePrueba.db")        
        cursor = con.cursor()
        cursor.execute("SELECT * FROM catalogo_netflix")
        rows = cursor.fetchall()
        cursor.close()
        con.commit()          
        return rows
    