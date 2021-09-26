import sqlite3

def base_netflix(ruta=True):
    if ruta == False:
        con = sqlite3.connect("BasePrueba.db")
    else:
        con = sqlite3.connect("DB/BasePrueba.db")
    cursor = con.cursor()
    cursor.execute('''CREATE TABLE catalogo_netflix(
                        catalogo_netflix number(100) PRIMARY KEY,
                        title VARCHAR2(2000),
                        director VARCHAR2(2000),
                        cast VARCHAR2(2000),
                        rating VARCHAR2(2000),
                        duration VARCHAR2(2000),
                        descripcion VARCHAR2(2000)
                )
                   ''')
    con.commit()
    con.close()

def base_multas(ruta=True):
    if ruta == False:
        con = sqlite3.connect("BasePrueba.db")
    else:
        con = sqlite3.connect("DB/BasePrueba.db")
    cursor = con.cursor()
    cursor.execute('''CREATE TABLE multas(
                        multa_id number(100) PRIMARY KEY,
                        anio VARCHAR2(2000),
                        periodo VARCHAR2(2000),
                        tipo_recurso VARCHAR2(2000),
                        tipo_pago integer,
                        costo number(2000),
                        descripcion_concepto VARCHAR2(2000),
                        monto_estimado number(2000),
                        monto_recaudado number(2000)
                )
                   ''')
    con.commit()
    con.close()
    