if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Retrieve the DataFrame from the 'data' dictionary
    final_quarter_df = data

    # Remove rows where passenger count is equal to 0 or trip distance is equal to 0
    final_quarter_df = final_quarter_df[(final_quarter_df['passenger_count'] > 0) & (final_quarter_df['trip_distance'] > 0)]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    final_quarter_df['lpep_pickup_date'] = final_quarter_df['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case
    final_quarter_df = final_quarter_df.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'ratecode_id', 'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'})

    # Assertions
    existing_values = [1, 2, 3]  # Replace with the actual list of existing values for vendor_id
    assert final_quarter_df['vendor_id'].isin(existing_values).all(), "Assertion Error: vendor_id is not one of the existing values."
    assert (final_quarter_df['passenger_count'] > 0).all(), "Assertion Error: passenger_count is not greater than 0."
    assert (final_quarter_df['trip_distance'] > 0).all(), "Assertion Error: trip_distance is not greater than 0."

    # Update the 'data' variable
    data = final_quarter_df

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
