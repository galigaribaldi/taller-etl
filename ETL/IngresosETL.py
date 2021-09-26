import pandas as pd
import sqlite3 
class IngresosETL():
    
    ruta = "Charge"
    nombreArchivo=""
    rutaCompleta = ""
    
    def __init__(self, ruta, nombreArchivo):
        self.ruta = ruta
        self.nombreArchivo = nombreArchivo
        self.rutaCompleta = self.ruta + "/"+self.nombreArchivo
        
    
    def extract_ingresos(self):
        archivo = pd.read_excel(self.rutaCompleta)
        ##Tratamiento de datos
        list_discriminante = archivo['tipo'].drop_duplicates()
        list_discriminante = list_discriminante.astype(int)
        list_discriminante = list(list_discriminante.unique())
        list_discriminante.sort()
        ###Retornar DF
        diccionarioDf = {}
        for i in list_discriminante:
            df = archivo[(archivo['tipo'] >= i) & (archivo['tipo'] <= i+1)]
            df = df.rename(columns={'costo_tipo_'+str(i):'costo_total'})
            df = df.reset_index(drop=True)
            df = df.fillna(0)
            diccionarioDf[i] = df
        return diccionarioDf
        #"""
    
    def generate_list_ingresos(self, dataframe):
        lista = list(tuple([
            int(i),
            str(dataframe['ciclo'][i]),
            str(dataframe['periodo'][i]),
            str(dataframe['tipo_recurso_clase'][i]),
            int(dataframe['tipo'][i]),
            int(dataframe['costo_total'][i]),
            str(dataframe['desc_concepto'][i]),
            int(dataframe['monto_estimado'][i]),
            int(dataframe['monto_recaudado'][i])
            ])for i in range(len(dataframe)))
        return lista
    
    def charge_ingresos_batch(self, lists,ruta=True):
        if ruta == False:
            con = sqlite3.connect("BasePrueba.db")
        else:
            con = sqlite3.connect("DB/BasePrueba.db")
            
        query = "INSERT INTO multas(multa_id,anio,periodo,tipo_recurso, tipo_pago,costo,descripcion_concepto,monto_estimado,monto_recaudado) VALUES (?,?,?,?,?,?,?,?,?)"        
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
        cursor.execute("SELECT * FROM multas")
        rows = cursor.fetchall()
        cursor.close()
        con.commit()          
        return rows
    