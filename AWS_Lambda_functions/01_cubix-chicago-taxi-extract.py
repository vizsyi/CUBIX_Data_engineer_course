from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import os
from typing import Dict, List

#import pandas as pd
import boto3
import requests

# 1. get one day's taxi data
# 2. get one day's weather data
# 3. upload to s3 (raw_data/to_processed/taxi_data and raw_data/to_processed/weather_data)

def formated_day_back(days: int):
    current_datetime = datetime.now() - relativedelta(days=days)
    return current_datetime.strftime("%Y-%m-%d")

def get_taxi_data (formated_datetime: str) -> List:
    """
    Fetches taxi trip data from the City of Chicago data portal for a specified date.

    Parameters:
        formatted_datetime (str): A formatted date string in 'YYYY-MM-DD' format.

    Returns:
        List: A list of dictionaries containing taxi trip data for the specified date.
    """
    
    url = "https://data.cityofchicago.org/resource/ajtu-isnz.json"
    params = f"$where=trip_start_timestamp>='{formated_datetime}T00:00:00' AND trip_start_timestamp<='{formated_datetime}T23:59:59'&$limit=30000"
    headers = {"X-App-Token": os.environ.get("CHICAGO_API_TOKEN")}

    response = requests.get(url, headers = headers, params = params)
    
    print (headers)
    return response.json()

def get_weather_data (formated_datetime: str) -> Dict:
    """
    Fetches weather data from the Open-Meteo archive API for a specified date and location.

    Parameters:
        formatted_datetime (str): A formatted date string in 'YYYY-MM-DD' format.

    Returns:
        Dict: A dictionary containing weather data including temperature, wind speed,
              rain, and precipitation for the specified date and location.
    """
    
    url = "https://archive-api.open-meteo.com/v1/era5"
    
    params = {
        "latitude": 41.85,
        "longitude": -87.65,
        "start_date": formated_datetime,
        "end_date": formated_datetime,
        "hourly": "temperature_2m,wind_speed_10m,rain,precipitation"
    }

    response = requests.get(url, params = params)
    return response.json()
    
def upload_to_s3(data, folder_name: str, filename: str) -> None:
    """
    Uploads data to an Amazon S3 bucket.

    Parameters:
        data: The data to be uploaded, either taxi or weather .
        folder_name (str): The name of the folder in the S3 bucket where the data will be stored.
        filename (str): The name of the file under which the data will be stored.

    Returns:
        None
    """
    
    client = boto3.client("s3")
    client.put_object(
        Bucket = "cubix-chicago-taxi-vi",
        Key = f"raw_data/to_processed/{folder_name}/{filename}",
        Body = json.dumps(data)
        )

def lambda_handler(event, context):
    formated_datetime = formated_day_back(60)
    
    taxi_data = get_taxi_data (formated_datetime)
    weather_data = get_weather_data (formated_datetime)

    taxi_filename = f"taxi_raw_{formated_datetime}.json"
    weather_filename = f"weather_raw_{formated_datetime}.json"
    
    upload_to_s3(data = taxi_data, filename = taxi_filename, folder_name = "taxi_data")
    print ("Taxi data has been uploaded!")
    
    upload_to_s3(data = weather_data, filename = weather_filename, folder_name = "weather_data")
    print ("Weather data has been uploaded!")
