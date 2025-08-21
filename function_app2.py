import os
import requests
from azure.storage.blob import BlobServiceClient
import time

# Azure Blob Storage SAS Token URL and Container Info
CONTAINER_NAME = 'capstone-datasets'
SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-08-30T18:36:00Z&st=2025-08-07T10:21:00Z&spr=https&sig=tLfnigXZAqa12T2qvWYvdEcPbNJWsdToq79cHUTS1FE%3D'
STORAGE_ACCOUNT_NAME = 'capstonestorexyz'

# Function to download and upload a single CSV
def download_and_upload_csv(file_name):
    file_url = f'https://www.weather.gov.sg/files/dailydata/DAILYDATA_S88_{file_name}.csv'
    
    try:
        response = requests.get(file_url)
        if response.status_code == 200:
            print(f'File {file_name}.csv exists, proceeding with download and upload...')

            # Connect to Azure Blob
            blob_service_client = BlobServiceClient(
                account_url=f'https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net',
                credential=SAS_TOKEN
            )
            container_client = blob_service_client.get_container_client(CONTAINER_NAME)
            blob_client = container_client.get_blob_client(f'DAILYDATA_S88_{file_name}.csv')

            # Upload file to Blob
            blob_client.upload_blob(response.content, overwrite=True)
            print(f'File {file_name}.csv uploaded successfully.')
        else:
            print(f'File {file_name}.csv does not exist at source. Status code: {response.status_code}')
    except requests.exceptions.RequestException as e:
        print(f"Request error for {file_url}: {e}")
    except Exception as e:
        print(f"Upload error for {file_name}.csv: {e}")

# Function to process all 12 months
def process_all_files():
    for month in range(1, 13):
        month_str = str(month).zfill(2)
        year = 2025
        file_name = f'{year}{month_str}'
        download_and_upload_csv(file_name)

# Main scheduler loop that runs every 15 seconds
if __name__ == '__main__':
    while True:
        print("\n--- Starting scheduled task ---")
        process_all_files()
        print("--- Task completed. Waiting 15 seconds... ---\n")
        time.sleep(15)
