# 10 years per request - max https://dev.meteostat.net/api/stations/daily

import ssl
import certifi

ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

from meteostat import Point, daily, stations
from datetime import date
import os
import pandas as pd
import math
import time
import sys
import json
from google.cloud import storage
from io import StringIO
from pathlib import Path

# Check are config files set.
config_directory = Path("/app/config")
required_files = ["config.json", "credentials.json"]

missing_files = []

for file_name in required_files:
    if not (config_directory / file_name).exists():
        missing_files.append(file_name)

if missing_files:
    raise FileNotFoundError(
        f"Missing required configuration files: {missing_files}"
    )


# Variables

all_stations_names = []
first_station_write = True
total_data_chunks = 0
processed_data_chunks = 0
root_dir = "weather_result"
seed_dir = "seeds"
all_stations_path = f"{root_dir}/all_stations_data.csv"
start_time = time.perf_counter()
# Buffer data for stations saving during capitals itereation
buffer = StringIO()


# Functions

# Create capitals list with period of 10 years
def create_capitals_list():
    capitals_df = pd.read_csv(f"{seed_dir}/capitals.csv")
    result = []

    start_year = 1970
    end_year = 2025

    decades = math.ceil((end_year - start_year) / 10)

    for row in capitals_df.itertuples():
        capital_id = row.id
        capital_name = row.capital
        capital_clean_name = capital_name.replace(" ", "_")
        lat = row.lat
        lon = row.lon
        
        for i in range(decades):
            start_period = start_year + i*10
            end_period = start_period + 9
            if end_period > end_year:
                end_period = end_year 
            result.append((capital_id, capital_clean_name, start_period, end_period, lat, lon))
    
    return result



# Load bucket data from config.json
with open("config/config.json") as f:
    config = json.load(f)


bucket_name = config["gcs"]["bucket"]


capitals_list = create_capitals_list()

client = storage.Client()
bucket = client.bucket(bucket_name)

for one_capital_chunk in capitals_list:
    
    capital_id = int(one_capital_chunk[0])
    capital_name = str(one_capital_chunk[1])
    start_year = int(one_capital_chunk[2])
    end_year = int(one_capital_chunk[3])
    latitude = float(one_capital_chunk[4])
    longitude = float(one_capital_chunk[5])

    working_directory = f"{root_dir}/{capital_name}"
    os.makedirs(working_directory, exist_ok=True)

    if capital_name not in all_stations_names:
        point = Point(latitude, longitude)
        station_df = stations.nearby(point, limit=1)
        station_df['capital_id'] = capital_id

        station_df.to_csv(
            buffer,
            header=first_station_write,
            index=True
        )

        first_station_write = False
        all_stations_names.append(capital_name)
    

    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)

    data = daily(station_df, start, end)
    daily_data = data.fetch()
    output_filename = f"{working_directory}/output-{capital_name}-{start_year}-{end_year}.csv"

    total_data_chunks = total_data_chunks + 1

    if daily_data is not None:
        # daily_data.to_csv(output_filename, index=True, encoding='utf-8')
        blob = bucket.blob(output_filename)

        csv_data = daily_data.to_csv(index=True, encoding="utf-8")
        blob.upload_from_string(csv_data, content_type="text/csv")
        processed_data_chunks = processed_data_chunks + 1

        print(f"Processed {capital_name} data for {start_year}-{end_year}")
    else:
        print(f"No data for {capital_name} for period {start_year}-{end_year}")


print(f"Poccessed chunks {processed_data_chunks} of {total_data_chunks}")


# Upload csv station data from buffer
blob = bucket.blob(f"{seed_dir}/all_stations_data.csv")


buffer.seek(0)

blob.upload_from_string(
    buffer.getvalue(),
    content_type="text/csv"
)

# Upload seeds from local dir
blob = bucket.blob(f"{seed_dir}/capitals.csv")
# Destination
blob.upload_from_filename(f"{seed_dir}/capitals.csv")

end_time = time.perf_counter()
print(f"[INGEST] Execution time: {end_time - start_time:.2f} seconds")
