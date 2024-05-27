import requests
import pandas as pd
from io import BytesIO
import boto3
from datetime import datetime

# Configuración de AWS
AWS_ACCESS_KEY_ID = 'tu_access_key'
AWS_SECRET_ACCESS_KEY = 'tu_secret_key'
AWS_STORAGE_BUCKET_NAME = 'tu_bucket_name'

def download_csv(url, encoding='utf-8'):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError on bad responses
        return pd.read_csv(BytesIO(response.content), encoding=encoding, on_bad_lines='skip')
    except UnicodeDecodeError as e:
        print(f"Error decoding CSV from {url}: {e}")
        raise
    except Exception as e:
        print(f"Error downloading CSV from {url}: {e}")
        raise

def save_to_s3(file_buffer, filename):
    try:
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        s3_resource.Object(AWS_STORAGE_BUCKET_NAME, filename).put(Body=file_buffer.getvalue())
        print(f"File {filename} uploaded to S3 bucket {AWS_STORAGE_BUCKET_NAME}.")
    except Exception as e:
        print(f"Error saving file to S3: {e}")
        raise

# URLs de los archivos CSV
cecotec_1_url = 'https://cecobi.cecotec.cloud/ws/getstockfeedb2c.php?etiqueta=DROPES'
cecotec_2_url = 'https://docs.google.com/spreadsheets/d/1g-fmRqM26vYUlyOiouIJiiU8Bt1PGTGLiMZZ4BrZU-8/edit?usp=sharing'
megasur_url = 'https://www.megasur.es/download/file-rate/?file=csv-prestashop&u=298707&hash=bb40b211b037dea6651b9892a9389978'
globomatik_url = 'https://multimedia.globomatik.net/csv/import.php?username=36268&password=87596029&mode=all&type=default'

# Descargar los archivos CSV
cecotec_1 = download_csv(cecotec_1_url, encoding='latin1')
cecotec_2 = download_csv(cecotec_2_url, encoding='latin1')
megasur = download_csv(megasur_url, encoding='latin1')
globomatik = download_csv(globomatik_url, encoding='latin1')

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

# Guardar el archivo Excel en S3
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
save_to_s3(excel_buffer, f'combined_catalog_{timestamp}.xlsx')

