import pandas as pd
import requests
from google.cloud import storage
from io import BytesIO
from google.auth import load_credentials_from_file

# Replace with your Google Cloud credentials file path
credentials_path = "de-zoomcamp24-5aaa1963845a.json"

# Set up Google Cloud Storage client
storage_client = storage.Client.from_service_account_json(credentials_path)

# Set your GCS bucket name
gcs_bucket_name = "de-zoomcamp-ny_taxi_data"

# Set the folder name
folder_name = "green_taxi_trip_2022"

# Base URL for the Parquet files
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-"

# Iterate through each month, download data from URL, and upload to GCS
for month in range(1, 13):
    parquet_url = f"{base_url}{str(month).zfill(2)}.parquet"
    
    try:
        response = requests.get(parquet_url)
        response.raise_for_status()  # Check for HTTP errors
        
        # Read the Parquet file from the response content
        parquet_data = BytesIO(response.content)
        
        # Get the original file name from the URL
        file_name = parquet_url.split("/")[-1]

        # Specify the GCS file name with the folder structure
        gcs_file_name = f"{folder_name}/{file_name}"
        
        # Upload the Parquet data to GCS with the specified folder structure
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_file_name)
        blob.upload_from_file(parquet_data, content_type='application/octet-stream')
        
        print(f"Data for month {str(month).zfill(2)} uploaded in: gs://{gcs_bucket_name}/{gcs_file_name}")
    except Exception as e:
        print(f"Error processing data for month {str(month).zfill(2)}: {e}")
