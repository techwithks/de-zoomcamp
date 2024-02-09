import pandas as pd
from google.cloud import bigquery
from google.auth import load_credentials_from_file

# Replace with your Google Cloud credentials file path
credentials_path = "de-zoomcamp24-key.json"

# Set up BigQuery client
client = bigquery.Client.from_service_account_json(credentials_path)

# Set your BigQuery dataset name
dataset_name = "green_taxi_dataset"
table_name = "trips_data_2022"

# Base URL for the Parquet files
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-"

# Initialize an empty dataframe to hold the concatenated data
combined_df = pd.DataFrame()

# Iterate through each month, construct the URL, and concatenate the data
for month in range(1, 13):
    parquet_url = f"{base_url}{str(month).zfill(2)}.parquet"
    try:
        df = pd.read_parquet(parquet_url)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    except Exception as e:
        print(f"Error reading data from {parquet_url}: {e}")

# Load credentials from the JSON key file
try:
    credentials, _ = load_credentials_from_file(credentials_path)
except Exception as e:
    print(f"Error loading credentials: {e}")
    credentials = None

# Upload the combined dataframe to BigQuery
try:
    combined_df.to_gbq(f"{dataset_name}.{table_name}", project_id=client.project, if_exists="replace", credentials=credentials)
    print("Data uploaded successfully.")
except Exception as e:
    print(f"Error uploading data to BigQuery: {e}")
