import requests
import pandas as pd
from io import BytesIO
import boto3
from datetime import datetime

# Configuración de AWS
AWS_ACCESS_KEY_ID = 'tu_access_key'
AWS_SECRET_ACCESS_KEY = 'tu_secret_key'
AWS_STORAGE_BUCKET_NAME = 'tu_bucket_name'

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError on bad responses
    return pd.read_csv(BytesIO(response.content), on_bad_lines='skip')

def save_to_s3(file_buffer, filename):
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_resource.Object(AWS_STORAGE_BUCKET_NAME, filename).put(Body=file_buffer.getvalue())

# URLs de los archivos CSV
cecotec_1_url = 'https://cecobi.cecotec.cloud/ws/getstockfeedb2c.php?etiqueta=DROPES'
cecotec_2_url = 'https://docs.google.com/spreadsheets/d/1g-fmRqM26vYUlyOiouIJiiU8Bt1PGTGLiMZZ4BrZU-8/edit?usp=sharing'
megasur_url = 'https://www.megasur.es/download/file-rate/?file=csv-prestashop&u=298707&hash=bb40b211b037dea6651b9892a9389978'
globomatik_url = 'https://multimedia.globomatik.net/csv/import.php?username=36268&password=87596029&mode=all&type=default'

# Descargar los archivos CSV
cecotec_1 = download_csv(cecotec_1_url)
cecotec_2 = download_csv(cecotec_2_url)
megasur = download_csv(megasur_url)
globomatik = download_csv(globomatik_url)

# Crear un archivo Excel con varias hojas
excel_buffer = BytesIO()
with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
    cecotec_1.to_excel(writer, sheet_name='Cecotec1', index=False)
    cecotec_2.to_excel(writer, sheet_name='Cecotec2', index=False)
    megasur.to_excel(writer, sheet_name='Megasur', index=False)
    globomatik.to_excel(writer, sheet_name='Globomatik', index=False)
    
    writer.save()

# Guardar el archivo Excel en S3
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
save_to_s3(excel_buffer, f'combined_catalog_{timestamp}.xlsx')

