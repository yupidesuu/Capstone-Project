import os
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import datetime
import time

# Azure Blob Storage SAS Token URL and Container Info
CONTAINER_NAME = 'capstone-datasets'
SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-08-30T18:36:00Z&st=2025-08-07T10:21:00Z&spr=https&sig=tLfnigXZAqa12T2qvWYvdEcPbNJWsdToq79cHUTS1FE%3D'
STORAGE_ACCOUNT_NAME = 'capstonestorexyz'
BLOB_URL = f'https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}?{SAS_TOKEN}'

# Function to download a file and upload it to Azure Blob Storage
def download_and_upload_csv(file_name):
    # Construct the URL to check if the file exists
    file_url = f'https://www.weather.gov.sg/files/dailydata/DAILYDATA_S88_{file_name}.csv'

    # Check if the file exists by sending a GET request
    try:
        response = requests.get(file_url)
        
        # If the response code is 200 (file exists)
        if response.status_code == 200:
            print(f'File {file_name}.csv exists, proceeding with download and upload...')
            
            # Connect to Azure Blob Storage using the SAS token
            blob_service_client = BlobServiceClient(account_url=f'https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net', credential=SAS_TOKEN)
            container_client = blob_service_client.get_container_client(CONTAINER_NAME)

            # Prepare the BlobClient to upload the file
            blob_client = container_client.get_blob_client(f'DAILYDATA_S88_{file_name}.csv')
            
            # Upload the file to Azure Blob Storage
            blob_client.upload_blob(response.content, overwrite=True)
            print(f'File {file_name}.csv uploaded successfully to Blob Storage.')
        else:
            # If the file does not exist, print a message and do nothing
            print(f'File {file_name}.csv does not exist at the source URL. Status code: {response.status_code}')
    
    except requests.exceptions.RequestException as e:
        # Handle network or request exceptions
        print(f"Error during request to {file_url}: {e}")
    except Exception as e:
        # Handle other general exceptions
        print(f"Error during blob upload for file {file_name}.csv: {e}")

# Main function to iterate through the months and process each file
def main():
    # Loop through months 202501 (Jan) to 202512 (Dec)
    for month in range(1, 13):  # January to December
        month_str = str(month).zfill(2)  # Ensure the month is two digits (e.g., "01", "02")
        year = 2025
        file_name = f'{year}{month_str}'
        
        # Call the function to download and upload the file for the current month
        download_and_upload_csv(file_name)

# Timer trigger will call this main function every 15 seconds
if __name__ == '__main__':
    main()