import requests
import pandas as pd
from io import StringIO
import boto3
from datetime import datetime
import openpyxl

# Configuración de AWS
AWS_ACCESS_KEY_ID = 'tu_access_key'
AWS_SECRET_ACCESS_KEY = 'tu_secret_key'
AWS_STORAGE_BUCKET_NAME = 'tu_bucket_name'

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError on bad responses
    return pd.read_csv(StringIO(response.text), on_bad_lines='skip')

def save_to_s3(file_buffer, filename):
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_resource.Object(AWS_STORAGE_BUCKET_NAME, filename).put(Body=file_buffer.getvalue())

# URLs de los archivos CSV
cecotec_1_url = 'url_del_csv_cecotec_1'
cecotec_2_url = 'url_del_csv_cecotec_2'
megasur_url = 'url_del_csv_megasur'
globomatik_url = 'url_del_csv_globomatik'

# Descargar los archivos CSV
cecotec_1 = download_csv(cecotec_1_url)
cecotec_2 = download_csv(cecotec_2_url)
megasur = download_csv(megasur_url)
globomatik = download_csv(globomatik_url)

# Crear un archivo Excel con varias hojas
excel_buffer = StringIO()
with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
    cecotec_1.to_excel(writer, sheet_name='Cecotec1', index=False)
    cecotec_2.to_excel(writer, sheet_name='Cecotec2', index=False)
    megasur.to_excel(writer, sheet_name='Megasur', index=False)
    globomatik.to_excel(writer, sheet_name='Globomatik', index=False)

# Guardar el archivo Excel en S3
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
save_to_s3(excel_buffer, f'combined_catalog_{timestamp}.xlsx')
