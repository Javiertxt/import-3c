import pandas as pd
import requests
from io import StringIO
import boto3
from botocore.exceptions import NoCredentialsError
import os

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    try:
        return pd.read_csv(StringIO(response.text), on_bad_lines='warn')
    except pd.errors.ParserError as e:
        print(f"Error parsing {url}: {e}")
        return pd.DataFrame()

# Obteniendo credenciales de las variables de entorno
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.environ.get('S3_BUCKET_NAME')

def upload_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    try:
        s3_client.upload_file(file_name, bucket, object_name or file_name)
        print(f"File {file_name} uploaded to {bucket}/{object_name or file_name}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

# Cecotec - File 1
cecotec_1_url = "https://cecobi.cecotec.cloud/ws/getstockfeedb2c.php?etiqueta=DROPES"
cecotec_1 = download_csv(cecotec_1_url)
cecotec_1['Proveedor'] = 'Cecotec'
cecotec_1 = cecotec_1.rename(columns={
    'public_id': 'SKU',
    'title': 'Nombre',
    'stock': 'Stock',
    'barcodes_ean_13': 'EAN'
})
cecotec_1 = cecotec_1[['Proveedor', 'EAN', 'Nombre', 'SKU', 'Stock']]

# Cecotec - File 2
cecotec_2_url = "https://docs.google.com/spreadsheets/d/1g-fmRqM26vYUlyOiouIJiiU8Bt1PGTGLiMZZ4BrZU-8/export?format=csv&id=1g-fmRqM26vYUlyOiouIJiiU8Bt1PGTGLiMZZ4BrZU-8&gid=0"
cecotec_2 = download_csv(cecotec_2_url)
cecotec_2 = cecotec_2.rename(columns={
    'DESCRIPCIÓN': 'Descripción',
    'PVPR': 'PVP'
})
cecotec_2['Precio de compra'] = (cecotec_2['H'] + cecotec_2['I']) * 1.21
cecotec_2 = cecotec_2[['EAN', 'Descripción', 'Precio de compra', 'PVP']]

# Merge Cecotec data
cecotec = pd.merge(cecotec_1, cecotec_2, on='EAN', how='left')

# Megasur
megasur_url = "https://www.megasur.es/download/file-rate/?file=csv-prestashop&u=298707&hash=bb40b211b037dea6651b9892a9389978"
megasur = download_csv(megasur_url)
megasur['Proveedor'] = 'Megasur'
megasur['Precio de compra'] = (4.5 + megasur['PVD'] + megasur['CANON']) * 1.21
megasur['PVP'] = (4.5 + megasur['PVP']) * 1.21
megasur = megasur.rename(columns={
    'name': 'Nombre',
    'STOCK': 'Stock',
    'EAN': 'EAN',
    'REF': 'SKU',
    'DESCRIPTION': 'Descripción',
    'URL_IMG': 'Imagen'
})
megasur = megasur[['Proveedor', 'EAN', 'Nombre', 'Descripción', 'SKU', 'Imagen', 'Stock', 'Precio de compra', 'PVP']]

# Globomatik
globomatik_url = "https://multimedia.globomatik.net/csv/import.php?username=36268&password=87596029&mode=all&type=default"
globomatik = download_csv(globomatik_url)
globomatik['Proveedor'] = 'Globomatik'
globomatik['Precio de compra'] = (4.5 + globomatik['PVD'] + globomatik['Canon']) * 1.21
globomatik = globomatik.rename(columns={
    'Descripcion': 'Nombre',
    'Stock': 'Stock',
    'EAN': 'EAN',
    'PN': 'SKU',
    'Desc. Larga': 'Descripción',
    'Imagen HD': 'Imagen'
})
globomatik = globomatik[['Proveedor', 'EAN', 'Nombre', 'Descripción', 'SKU', 'Imagen', 'Stock', 'Precio de compra']]

# Combine all data
combined = pd.concat([cecotec, megasur, globomatik], ignore_index=True)

# Deduplicate
combined = combined.sort_values(by=['EAN', 'Precio de compra', 'Stock'], ascending=[True, True, False])
combined = combined.drop_duplicates(subset='EAN', keep='first')

# Save to file
csv_file = 'combined_catalog.csv'
xlsx_file = 'combined_catalog.xlsx'
combined.to_csv(csv_file, index=False)
combined.to_excel(xlsx_file, index=False)

# Upload to S3
s3_bucket = os.environ.get('S3_BUCKET_NAME')
upload_to_s3(csv_file, s3_bucket)
upload_to_s3(xlsx_file, s3_bucket)

print("Combined catalog has been created and uploaded successfully.")
