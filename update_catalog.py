import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# Deshabilitar advertencias de verificación SSL
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_csv(url, encoding='utf-8'):
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return response.content.decode(encoding)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading CSV from {url}: {e}")
        raise

def download_google_sheet(url, sheet_name=0):
    try:
        sheet_url = url.replace('/edit?usp=sharing', '/export?format=csv&gid=0')
        response = requests.get(sheet_url, verify=False)
        response.raise_for_status()
        return response.content.decode('utf-8')
    except requests.exceptions.RequestException as e:
        print(f"Error downloading Google Sheet from {url}: {e}")
        raise

def csv_to_dataframe(csv_content):
    from io import StringIO
    return pd.read_csv(StringIO(csv_content))

def save_to_database(df, table_name, connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)

# URLs de los archivos CSV
cecotec_1_url = 'https://cecobi.cecotec.cloud/ws/getstockfeedb2c.php?etiqueta=DROPES'
cecotec_2_url = 'https://docs.google.com/spreadsheets/d/1g-fmRqM26vYUlyOiouIJiiU8Bt1PGTGLiMZZ4BrZU-8/edit?usp=sharing'
megasur_url = 'https://www.megasur.es/download/file-rate/?file=csv-prestashop&u=298707&hash=bb40b211b037dea6651b9892a9389978'
globomatik_url = 'https://multimedia.globomatik.net/csv/import.php?username=36268&password=87596029&mode=all&type=default'

# Descargar los archivos CSV
cecotec_1 = download_csv(cecotec_1_url)
cecotec_2 = download_google_sheet(cecotec_2_url)
megasur = download_csv(megasur_url)
globomatik = download_csv(globomatik_url, encoding='latin1')

# Convertir CSV a DataFrame
cecotec_1_df = csv_to_dataframe(cecotec_1)
cecotec_2_df = csv_to_dataframe(cecotec_2)
megasur_df = csv_to_dataframe(megasur)
globomatik_df = csv_to_dataframe(globomatik)

# Guardar DataFrame en la base de datos
db_connection_string = os.getenv('DATABASE_URL', 'mysql+pymysql://user:password@localhost/dbname')
save_to_database(cecotec_1_df, 'cecotec_1', db_connection_string)
save_to_database(cecotec_2_df, 'cecotec_2', db_connection_string)
save_to_database(megasur_df, 'megasur', db_connection_string)
save_to_database(globomatik_df, 'globomatik', db_connection_string)

print("Proceso completado")


