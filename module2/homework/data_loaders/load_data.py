import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    # List to store DataFrames for each month
    data_frames = []

    # Loop through months 10, 11, and 12
    for month in range(10, 13):
        # Format the URL for each month
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month:02d}.csv.gz"
        
        # Read data into DataFrame
        df = pd.read_csv(url, parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime'], low_memory=False)
        
        # Append the DataFrame to the list
        data_frames.append(df)

    # Concatenate DataFrames for the final quarter
    final_quarter_df = pd.concat(data_frames, ignore_index=True)

    return final_quarter_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
