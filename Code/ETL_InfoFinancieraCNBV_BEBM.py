# Proceso de Transformacion, Limpieza y Carga de Datos para el Modelo de Informacion Financiera CNBV
# Version 01
# 2025-06-24

import sys                                                  # Libreria que nos permite obtener los Parametros declarados en el comando Bash que llama a este Script.
import xlwings as xw                                        # Lectura de Archivos Excel.
import pandas as pd                                         # Manipulacion de Estructuras de Datos.
import numpy as np                                          # Procesamiento de Datos y Funciones Matematicas de alto nivel.
import psycopg2 as psy2                                     # Adaptador y Controlador de Bases de Datos PostgreSQL.
from psycopg2.extensions import register_adapter, AsIs      # Extensiones de psycopg2 para hacer compatibles los Tipos de Datos de los Data Frames manipulados en Python con los Correspondientes a PostgreSQL. 

# Ejecutamos los siguientes comandos para hacer compatibles los Tipos de Datos de los Data Frames Intermedios que crearemos en Python con los Tipos de Datos de la BD de Destino.

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)

# Asignamos a una variable el Valor del Parametro que representa la Fecha del Reporte de la CNBV a procesar y que pasaremos a este Script a la hora de ejecutarlo.

if __name__ == "__main__":
    if len(sys.argv) > 1:
        archivoproceso = sys.argv[1]

# Asignamos a una variable la Ruta donde se alojan los Archivos del Boletin Estadistico extraidos desde la Pagina de la CNBV.

dataDirectoryLanding = r'/Users/david/Documents/Github/DataPipeline_InformacionFinanciera/Data'

# Nos conectamos a la BD de Destino para extraer la Informacion de los Catalogos de los Tipos de Cartera y los Bancos para estandarizar la informacion asignando los Ids correspondientes a la Tabla Fact.

InfoFinancieraConnetion = psy2.connect(
    host = "localhost",
    port = 5432,
    database = "InfoFinanciera",
    user = "postgres",
    password = "xxxx"
)
InfoFinanciera_Cursor = InfoFinancieraConnetion.cursor()

qryCatTiposCartera = 'SELECT "IdTipoCartera", "NombreCortoCartera" FROM "InfoCNBV"."BE_BM_Dim_CatTiposCarteras" ORDER BY "IdTipoCartera";'
dfCatTiposCartera = pd.read_sql_query(qryCatTiposCartera, InfoFinancieraConnetion)
TiposCartera_List = dfCatTiposCartera["NombreCortoCartera"].tolist()
IdsTiposCartera_List = dfCatTiposCartera["IdTipoCartera"].tolist()

qryCatBancos = 'SELECT * FROM "InfoCNBV"."BE_BM_Dim_CatBancos";'
dfCatBancos = pd.read_sql_query(qryCatBancos, InfoFinancieraConnetion)

# En caso de que existan, eliminamos los Registros cuya fecha es la que estamos Procesando.

qryDeleteInfo = """DELETE FROM "InfoCNBV"."BE_BM_Fact_SaldosCarteraCredito" WHERE "FechaReporte" = '"""+archivoproceso[0:4]+'-'+archivoproceso[4:6]+'-01'+"';"
InfoFinanciera_Cursor.execute(qryDeleteInfo)    
InfoFinancieraConnetion.commit()

# Cerramos la coneccion a la BD

InfoFinanciera_Cursor.close()
InfoFinancieraConnetion.close()

# Procesamos la Informacion Financiera por cada Hoja correspondiente a las siguientes Carteras mediante un "For Loop":

# - Cartera de Credito Total
# - Cartera de Credito a Empresas
# - Cartera de Credito a Entidades Financieras
# - Cartera de Credito a Entidades Gubernamentales
# - Cartera de Credito a Estados y Municipios
# - Cartera de Credito a Otras Entidades Gubernamentales
# - Cartera de Credito al Consumo (Total)

dataFramesSaldosCarteras = []

for cartera in IdsTiposCartera_List:
    CCiterationwb = xw.Book(dataDirectoryLanding+'BE_BM_'+archivoproceso+'.xlsx').sheets[TiposCartera_List[cartera]]
    landingCCiterationList = CCiterationwb.range("B6:E60").value

    feMensual = landingCCiterationList[0][3]
    feMensualString = feMensual.strftime("%Y-%m-%d")

    trustedCCiterationList = []

    for banco in trustedCCiterationList:
        bancoCleaned = (feMensualString, cartera, banco[0], banco[3])
        trustedCCiterationList.append(bancoCleaned)

    del trustedCCiterationList[0:2]

    trustedCCiterationDataFrame = pd.DataFrame(trustedCCiterationList, columns = ['FECHA MENSUAL', 'TIPO CARTERA', 'BANCO', 'CARTERA TOTAL'])
    trustedCCiterationDataFrame['CARTERA TOTAL'] = trustedCCiterationDataFrame['CARTERA TOTAL'].replace('n. a.', 0)
    trustedCCiterationDataFrame = trustedCCiterationDataFrame[trustedCCiterationDataFrame['BANCO'].notna()]

    dataFramesSaldosCarteras.append(trustedCCiterationDataFrame)

# Concentramos la informacion de los Data Frame creados para cada Cartera en uno solo.

cleanedDataFrame = pd.concat(dataFramesSaldosCarteras, ignore_index = True)

# Estandarizamos la Informacion agregando Id's correspondientes a cada Instutcion Bancaria mediante el cruce con el Catalogo de Bancos previamente extraido.

surfaceDataFrame = pd.merge(dfCatBancos, cleanedDataFrame, left_on = 'NombreBanco', right_on = 'BANCO')
surfaceDataFrame = surfaceDataFrame[['FECHA MENSUAL', 'TIPO CARTERA', 'IdBanco', 'CARTERA TOTAL']]

# Insertamos los registros de nuestro Data Frame estandarizado y Limpio mediante un For Loop a nuestra tabla Destino dentro de la BD PostgreSQL.

nrowsCCFinalDataFrame = list(range(0, len(surfaceDataFrame)))

for i  in nrowsCCFinalDataFrame:

    try:
        InfoFinancieraConnecion = psy2.connect(
            host = "localhost",
            port = 5432,
            database = "InformacionFinanciera",
            user = "postgres",
            password = "xxxx"
            )
        InfoFinanciera_Cursor = InfoFinancieraConnecion.cursor()

        insertCommand = """INSERT INTO "InfoCNBV"."BE_BM_Fact_SaldosCarteraCredito"("IdTipoCartera", "IdBanco", "MontoCartera", "IMOR", "ICOR", "FechaReporte") VALUES (%s, %s, %s, %s, %s, %s);"""
        insertRow = surfaceDataFrame.iloc[i].tolist()
        InfoFinanciera_Cursor.execute(insertCommand, insertRow)
        InfoFinancieraConnecion.commit()

    except(Exception, psy2.Error) as error:
        print('Error al insertar la Fila en la BD', error)

    finally:

        if InfoFinancieraConnecion:
            InfoFinanciera_Cursor.close()
            InfoFinancieraConnecion.close()

# Una vez finalizada la insercion de todos los registros, termina el Proceso de Transformacion y Carga de Datos.

print('Finaliza Proceso de Limpieza, Transformacion y Carga de Datos')