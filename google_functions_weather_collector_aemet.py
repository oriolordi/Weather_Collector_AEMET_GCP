# Weather collector (Google Functions version)

# Author: Oriol Ordi

# This script is meant to be run on Google Functions,
# together will Google Cloud Scheduler to be run daily

# The script collects hourly weather data from different spanish cities
# The data collected is from the AEMET API
# The data collected is dumped into Google Storage

# Note: remember to add the requirements.txt to the Google Function and
# to define the main function as the entry point


# Import statements
import requests
import pandas as pd
from google.cloud import storage
from datetime import date


# Define the function to collect data from the AEMET API
def get_weather_df(complete_url):
    # Get the data from the requests.get method
    json_access = requests.get(complete_url).json()
    # Get the list of dictionaries of hourly data
    json_weather = requests.get(json_access['datos']).json()
    # Convert the list of dictionaries to a pandas dataframe
    df_weather = pd.DataFrame(json_weather)
    # Set the time as index and sort by the time (the index)
    df_weather.set_index('fint', inplace=True)
    df_weather.sort_index(inplace=True)

    return df_weather



# Define the function to upload the weather information to Google Storage
# This function saves each dataframe (for each city) containing the weather
# for the day into a Googl Storage csv file
def upload_dfs_to_google_storage(dict_dfs_weather, bucket_name):
    # Create the storage client from the service account key in the json file
    client = storage.Client()
    # Access the bucket where we will upload the files
    bucket = client.get_bucket(bucket_name)
    # Get year, month and day
    the_date = date.today().strftime('%Y-%m-%d')
    the_year = the_date.split('-')[0]
    the_month = the_date.split('-')[1]
    the_day = the_date.split('-')[2]
    # Create a file for each of the dataframes
    for city, df in dict_dfs_weather.items():
        # Create the name of the file
        file_name = 'GoogleFunctions/' + city + '/' + the_year + '/' + the_month + '/' + the_day + '.csv'
        # Select the file_name destination in Google Storage
        object = bucket.blob(file_name)
        # Upload the pandas dataframe to the file in Google Storage
        object.upload_from_string(df.to_csv(), 'text/csv')


# Define the main function
def main(content, event):
    # Set up the url for the data as well as the API key for AEMET data
    stations = {'Barcelona': '0201D', 'Madrid': '3195'}
    url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/'
    api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvcmlvbC5vcmRpQGdtYWlsLmNvbSIsImp0aSI6IjQ0NjYyNzRiLTZjZTEtNDQ4OS1i' \
            'NWVjLTZjMjVmNDBkNTU4YiIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTk4ODgxNzQ0LCJ1c2VySWQiOiI0NDY2Mjc0Yi02Y2UxLTQ0ODktY' \
            'jVlYy02YzI1ZjQwZDU1OGIiLCJyb2xlIjoiIn0.wgkrh9IMh_tkUEOhu234JemE5Mex2xOCHIgDC1Z5Bzw'
    # Define the name of the S3 bucket where the data will be dumped
    bucket_name = "weather_aemet_bucket"

    # Iterate for every city and get the data from AEMET
    dict_dfs_weather = {}
    for city, city_code in stations.items():
        # Get the complete URL for the requests.get method
        complete_url = url + city_code + '/?api_key=' + api_key
        # Call the function to connect with the AEMET API and get the wheather info
        df = get_weather_df(complete_url)
        # Add the dataframe to the dictionary with the key being the city name
        dict_dfs_weather[city] = df

    # Check if the program managed to extract any dataframes
    if bool(dict_dfs_weather):
        # Call the function to upload the files to Google Storage
        upload_dfs_to_google_storage(dict_dfs_weather, bucket_name)
