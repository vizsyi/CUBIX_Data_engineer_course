from io import StringIO
import json

import boto3
import pandas as pd

def taxi_trips_transformations(taxi_trips: pd.DataFrame) -> pd.DataFrame:
    
    """ Performs transformations on a DataFrame containing taxi trip data.

    Parameters:
        taxi_trips (pd.DataFrame): A DataFrame containing taxi trip data.

    Returns:
        pd.DataFrame: The cleaned, transformed DataFrame.
    """

    if not isinstance(taxi_trips, pd.DataFrame):
        raise TypeError("taxi_trips is not a valid pandas Dataframe.")
    
    taxi_trips.drop(["pickup_census_tract", "dropoff_census_tract", "pickup_centroid_location", "dropoff_centroid_location"],
                    axis=1, inplace=True)

    taxi_trips.dropna(inplace=True)

    taxi_trips.rename(columns={"pickup_community_area": "pickup_community_area_id",
                            "dropoff_community_area": "dropoff_community_area_id"}, inplace=True)

    taxi_trips["trip_start_timestamp"] = pd.to_datetime(taxi_trips["trip_start_timestamp"])
    taxi_trips["trip_end_timestamp"] = pd.to_datetime(taxi_trips["trip_end_timestamp"])

    taxi_trips["datetime_for_weather"] = taxi_trips["trip_start_timestamp"].dt.floor("h")

    return taxi_trips
    
def update_taxi_trips_with_master_data(taxi_trips: pd.DataFrame, payment_type_master: pd.DataFrame, company_master: pd.DataFrame) -> pd.DataFrame:
    """
    Updates a taxi trips DataFrame with the payment type and company master ids and delete the string columns.

    Parameters:
        taxi_trips (pd.DataFrame): A DataFrame containing taxi trip data.
        payment_type_master (pd.DataFrame): A DataFrame containing the payment type master data.
        company_master (pd.DataFrame): A DataFrame containing the company master data.

    Returns:
        pd.DataFrame: The updated DataFrame with payment type and company ids merged,
                      and the original payment type and company string columns dropped.
    """
    
    taxi_trips_id = taxi_trips.merge(payment_type_master, on="payment_type")
    taxi_trips_id = taxi_trips_id.merge(company_master, on="company")

    taxi_trips_id.drop(["payment_type", "company"], axis = 1, inplace = True)

    return taxi_trips_id

def update_master(taxi_trips: pd.DataFrame, master: pd.DataFrame, id_column: str, value_column: str) -> pd.DataFrame:
    """
    Extends the master DataFrame with new values found in the given taxi trips data.

    Args:
    - taxi_trips (DataFrame): the daily taxi trips data, where each row represents a trip.
    - payment_type_master (DataFrame): the master list of taxi payment types.

    Returns:
    - updated_master (DataFrame): the updated master list
        after adding new values found in the taxi trips data.
    - id_column (str): the id column of the master DataFrame
    - value_column (str): the value column of the master DataFrame
    """

    max_id = master[id_column].max()
    
    new_values_list = \
        [value for value in taxi_trips[value_column].values if value not in master[value_column].values]
    
    new_values_df = pd.DataFrame({
        id_column: range(max_id + 1, max_id + len(new_values_list) + 1),
        value_column: new_values_list
    })

    updated_master = pd.concat([master, new_values_df], ignore_index = True)

    return updated_master

def transform_weather_data(weather_data: json) -> pd.DataFrame:
    """
    Transforms raw weather data from a JSON object into a formatted Pandas DataFrame.

    Parameters:
        weather_data (json): Hourly weather data from the Open Meteo API.

    Returns:
        pd.DataFrame: A DataFrame containing the transformed weather data with renamed columns.
    """

    weather_df = pd.DataFrame(weather_data['hourly'])

    weather_df['time'] = pd.to_datetime(weather_df['time'])

    # translate headers based on dictionary
    header_dict = {'time': 'datetime',
        'temperature_2m': 'temperature',
        'wind_speed_10m': 'wind_speed'}
    weather_df.columns = pd.Series(weather_df.columns).replace(header_dict)

    return weather_df

def read_csv_from_s3(bucket: str, path: str, filename: str) -> pd.DataFrame:
    """
    Reads a CSV file from an Amazon S3 bucket and returns it as a Pandas DataFrame.

    Parameters:
        bucket (str): The name of the S3 bucket.
        path (str): The path within the S3 bucket where the file is located.
        filename (str): The name of the CSV file to be read.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the CSV file.
    """
    
    s3 = boto3.client("s3")
    
    full_path = f"{path}{filename}"

    object = s3.get_object(Bucket = bucket, Key = full_path)
    object = object["Body"].read().decode("utf-8")
    output_df = pd.read_csv(StringIO(object))
    
    return output_df

def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Reads a JSON file from an Amazon S3 bucket and returns it as a dictionary.

    Parameters:
        bucket (str): The name of the S3 bucket.
        key (str): The key (path) of the JSON file in the S3 bucket.

    Returns:
        dict: A dictionary containing the data from the JSON file.
    """

    s3 = boto3.client("s3")
    
    response = s3.get_object(Bucket = bucket, Key = key)
    content = response['Body']
    json_dict = json.loads(content.read())

    return json_dict

def upload_dataframe_to_s3(dataframe: pd.DataFrame, bucket: str, path: str):
    """
    Uploads a Pandas DataFrame to an Amazon S3 bucket as a CSV file.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to be uploaded.
        bucket (str): The name of the S3 bucket where the file will be stored.
        path (str): The path in the S3 bucket where the file will be stored.

    Returns:
        None
    """
    
    s3 = boto3.client("s3")
    
    buffer = StringIO()
    dataframe.to_csv(buffer, index = False)
    df_content = buffer.getvalue()
    s3.put_object(Bucket = bucket, Key = path, Body = df_content)
    
def upload_master_data_to_s3(dataframe: pd.DataFrame, bucket: str, file_type: str, path: str):
    """
    Uploads a DataFrame to an Amazon S3 bucket as the master data file, preserving the previous version.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to be uploaded.
        bucket (str): The name of the S3 bucket.
        file_type (str): Either "company" or "payment_type".
        path (str): The path in the S3 bucket where the master data file will be stored.

    Returns:
        None
    """
    
    s3 = boto3.client("s3")
    
    master_file_path = f"{path}{file_type}_master.csv"
    previous_master_file_path = f"transformed_data/master_table_previous_version/{file_type}_master_previous_version.csv"
    
    s3.copy_object(
        Bucket = bucket,
        CopySource = {"Bucket": bucket, "Key": master_file_path},
        Key = previous_master_file_path
    )
        
    upload_dataframe_to_s3(dataframe = dataframe, bucket = bucket, path = master_file_path)

def upload_and_move_file_on_s3(
    dataframe: pd.DataFrame,
    datetime_col: str,
    bucket: str,
    file_type: str,
    source_path: str,
    processed_path: str,
    transformed_path: str,
    filename: str
    ):

    """
    Uploads a DataFrame to an Amazon S3 bucket, then moves the source file from the source folder to another.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to be uploaded.
        datetime_col (str): The name of the datetime column in the DataFrame used for formatting the filename.
        bucket (str): The name of the S3 bucket.
        file_type (str): The type of file to be uploaded. Either "taxi_trips" or "weather".
        source_path (str): The S3 path where the source file is located.
        processed_path (str): The S3 path where the source file will be moved after processing.
        transformed_path (str): The S3 path where the transformed DataFrame will be uploaded.
        filename (str): The name of the source file to be moved.

    Returns:
        None
    """
    
    s3 = boto3.client("s3")
    
    formatted_date = dataframe[datetime_col].iloc[0].strftime("%y-%m-%d")
    transformed_filepath = f"{transformed_path}{file_type}_{formatted_date}.csv"
    
    upload_dataframe_to_s3(dataframe = dataframe, bucket = bucket, path = transformed_filepath)
    
    s3.copy_object(
        Bucket = bucket,
        CopySource = {"Bucket": bucket, "Key": f"{source_path}{filename}"},
        Key = f"{processed_path}{filename}"
    )
        
    s3.delete_object(Bucket = bucket, Key = f"{source_path}{filename}")
    
    print(f"{filename} has been uploaded and moved")

#
# MAIN FUNCTION
#

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    
    bucket = "cubix-chicago-taxi-vi"
    raw_taxi_trips_folder = "raw_data/to_processed/taxi_data/"
    raw_weather_folder = "raw_data/to_processed/weather_data/"
    processed_taxi_trips_folder = "raw_data/processed/taxi_data/"
    processed_weather_folder = "raw_data/processed/weather_data/"
    transformed_taxi_trips_folder = "transformed_data/taxi_trips/"
    transformed_weather_folder = "transformed_data/weather/"

    payment_type_master_folder = "transformed_data/payment_type/"
    company_master_folder = "transformed_data/company/"
    
    payment_type_master_filename = "payment_type_master.csv"
    company_master_filename = "company_master.csv"
    
    payment_type_master = read_csv_from_s3(bucket = bucket, path = payment_type_master_folder, filename = payment_type_master_filename)
    company_master = read_csv_from_s3(bucket = bucket, path = company_master_folder, filename = company_master_filename)
    
    print(company_master.columns, company_master.shape)
    print(payment_type_master.columns, payment_type_master.shape)
    
    # TAXI DATA TRANSFORMATION AND LOADING
    for file in s3.list_objects(Bucket = bucket, Prefix = raw_taxi_trips_folder)['Contents']:
        taxi_trips_key = file['Key']
        
        filename = taxi_trips_key.split("/")[-1].strip()
        
        if filename != "":
            if taxi_trips_key.split(".")[-1] == "json":

                taxi_trips_data_json = read_json_from_s3(bucket = bucket, key = taxi_trips_key)
                
                taxi_trips_data_raw = pd.DataFrame(taxi_trips_data_json)
                taxi_trips = taxi_trips_transformations(taxi_trips_data_raw)

                company_master = update_master(taxi_trips, company_master, "company_id", "company")
                payment_type_master = update_master(taxi_trips, payment_type_master, "payment_type_id", "payment_type")
                
                print(company_master.columns, company_master.shape)
                print(payment_type_master.columns, payment_type_master.shape)

                taxi_trips = update_taxi_trips_with_master_data(taxi_trips, payment_type_master, company_master)
                
                upload_master_data_to_s3(bucket = bucket, path = payment_type_master_folder,
                    file_type = "payment_type", dataframe = payment_type_master)
                upload_master_data_to_s3(bucket = bucket, path = company_master_folder,
                    file_type = "company", dataframe = company_master)
                    
                upload_and_move_file_on_s3(
                    dataframe = taxi_trips,
                    datetime_col = "datetime_for_weather",
                    bucket = bucket,
                    file_type = "taxi_trips",
                    source_path = raw_taxi_trips_folder,
                    processed_path = processed_taxi_trips_folder,
                    transformed_path = transformed_taxi_trips_folder,
                    filename = filename)


    # WEATHER TRANSFORMATION AND LOADING
    for file in s3.list_objects(Bucket = bucket, Prefix = raw_weather_folder)['Contents']:
        weather_key = file['Key']
        
        filename = weather_key.split("/")[-1].strip()
        
        if filename != "":
            if weather_key.split(".")[-1] == "json":
                
                weather_data_json = read_json_from_s3(bucket = bucket, key = weather_key)

                weather_data = transform_weather_data(weather_data_json)
                
                print(weather_data.columns, weather_data.shape)
                
                upload_and_move_file_on_s3(
                    dataframe = weather_data,
                    datetime_col = "datetime",
                    bucket = bucket,
                    file_type = "weather",
                    source_path = raw_weather_folder,
                    processed_path = processed_weather_folder,
                    transformed_path = transformed_weather_folder,
                    filename = filename)
                
