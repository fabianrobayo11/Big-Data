# importar librerias para manipulacion de datos
import numpy  as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path
import logging
from dateutil.parser import parse
from pathlib import Path

bucket = 'gs://r11_bucket_llamadas123'

def col_name (x):
    return x.replace('-','_')


def main(): 
    logger = logging.getLogger('main')
    filename  = "llamadas_123_febrero2021.csv"
    # leer archivo
    #filemane = filemane.rename(col_name, axis = 'columns')
    data = get_data(filename = filename).rename(col_name, axis = 'columns')
    # saca resumen
    df_resumen = get_summary(data)
    # guarda resumen
    save_data(df_resumen, filename)



    
def save_data(df, filename):
    logger = logging.getLogger('save_data')
    # Guardar la tabla
    out_name = 'Etl_Limpieza_' + filename
    bucket = 'gs://r11_bucket_llamadas123'
    out_path = os.path.join(bucket, 'data', 'processed', out_name)
    df.to_csv(out_path)
    df.to_gbq(destination_table='big_data_llamadas123.consolidado123', if_exists='append')
    
       
    
def get_summary(data):
    logger = logging.getLogger('get_summary')
    
    # Craer unn diccionario vacio
    dict_resume= dict()

    for col in data.columns:
                      
        data = data.drop_duplicates()
        
        data = data.fillna({'UNIDAD': 'SIN_DATO'})

        col = 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
        data[col] = pd.to_datetime(data[col], errors='coerce')
            
        data['RECEPCION'] = pd.to_datetime(data['RECEPCION'], errors='coerce')

        data['EDAD'] = data['EDAD'].replace({'SIN_DATO' : np.nan})

        f = lambda x: x if pd.isna(x) == True else int(x)
        data['EDAD'] = data['EDAD'].apply(f)

        df = pd.DataFrame(data,columns=['CODIGO_LOCALIDAD','LOCALIDAD'])

        df.loc[df['CODIGO_LOCALIDAD']==1,'LOCALIDAD']='Usaquen'
        df.loc[df['CODIGO_LOCALIDAD']==2,'LOCALIDAD']='Chapinero'
        df.loc[df['CODIGO_LOCALIDAD']==3,'LOCALIDAD']='Santa Fe'
        df.loc[df['CODIGO_LOCALIDAD']==4,'LOCALIDAD']='San Cristobal'
        df.loc[df['CODIGO_LOCALIDAD']==5,'LOCALIDAD']='Usme'
        df.loc[df['CODIGO_LOCALIDAD']==6,'LOCALIDAD']='Tunjuelito'
        df.loc[df['CODIGO_LOCALIDAD']==9,'LOCALIDAD']='Fontibon'
        df.loc[df['CODIGO_LOCALIDAD']==10,'LOCALIDAD']='Engativa'
        df.loc[df['CODIGO_LOCALIDAD']==14,'LOCALIDAD']='Los Martires'
        df.loc[df['CODIGO_LOCALIDAD']==15,'LOCALIDAD']='Antonio Nariño'
        df.loc[df['CODIGO_LOCALIDAD']==19,'LOCALIDAD']='Ciudad Bolivar'

        data['LOCALIDAD'] = df['LOCALIDAD']

        df_resumen = data

        return df_resumen
    
def get_data(filename):
    logger = logging.getLogger('get_data')
    data_dir = "raw"
    bucket = 'gs://r11_bucket_llamadas123'
    file_path = os.path.join(bucket, "data", data_dir, filename)
    logger.info(f'Reading file: {file_path}')
    data = pd.read_csv(file_path, encoding='latin-1', sep=';')
    logger.info(f'la tabla contiene {data.shape[0]} filas y {data.shape[1]} columnas')
    return data

if __name__ == '__main__':
    main()
    
print('final')