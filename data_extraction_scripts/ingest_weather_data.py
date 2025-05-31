import requests
import json
import os
from datetime import datetime
from google.cloud import storage

# --- Configuration ---
RAW_DATA_BUCKET_NAME = "tet-raw-data"  # Just the bucket name, without "gs://"
WEATHER_DATA_GCS_PREFIX = "weather_data"
TARGET_YEAR = 2022  # The year for which to fetch data

# Define the cities and their precise coordinates
CITIES_TO_INGEST = [
    {"name": "Medellin", "latitude": 6.2214413, "longitude": -75.55185},
    {"name": "Bogota", "latitude": 4.711, "longitude": -74.0721},
    {"name": "Cali", "latitude": 3.4516, "longitude": -76.532},
    {"name": "Barranquilla", "latitude": 10.9685, "longitude": -74.1633},
    {"name": "Cartagena", "latitude": 10.391, "longitude": -75.4794},
]

# --- CRITICAL FIX: Changed BASE_OPEN_METEO_URL to the ARCHIVE API endpoint ---
BASE_OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
# --- End CRITICAL FIX ---

TIMEZONE = "America/Bogota"  # Keeping consistent timezone for Colombian cities
DAILY_PARAMS = "temperature_2m_max,precipitation_sum"


def ingest_weather_data():
    print(
        f"Starting weather data ingestion for year {TARGET_YEAR} using the Open-Meteo ARCHIVE API..."
    )

    start_date = f"{TARGET_YEAR}-01-01"
    end_date = f"{TARGET_YEAR}-12-31"

    # Initialize GCS client outside the loop for efficiency
    storage_client = storage.Client()
    bucket = storage_client.bucket(RAW_DATA_BUCKET_NAME)

    for city in CITIES_TO_INGEST:
        city_name = city["name"]
        latitude = city["latitude"]
        longitude = city["longitude"]

        print(f"Fetching data for {city_name} (Lat: {latitude}, Lon: {longitude})...")

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": DAILY_PARAMS,
            "timezone": TIMEZONE,
            "start_date": start_date,
            "end_date": end_date,
        }

        try:
            response = requests.get(BASE_OPEN_METEO_URL, params=params)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            weather_data = response.json()

            # Check if the 'daily' data is present and has 'time' entries
            if not weather_data.get("daily") or not weather_data["daily"].get("time"):
                print(
                    f"WARNING: No daily weather data found for {city_name} in the API response. "
                    f"Please check the city coordinates or API parameters. Raw response keys: {weather_data.keys()}"
                )
                continue  # Skip to the next city if no data was returned

            # Construct the unique output path for each city's JSON file in GCS
            # Example: weather_data/2022/medellin_2022.json
            output_filename = (
                f"{city_name.lower().replace(' ', '_')}_{TARGET_YEAR}.json"
            )
            blob_path = f"{WEATHER_DATA_GCS_PREFIX}/{TARGET_YEAR}/{output_filename}"

            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                json.dumps(weather_data, indent=2),  # Pretty print JSON
                content_type="application/json",
            )

            print(
                f"Successfully ingested data for {city_name} to gs://{RAW_DATA_BUCKET_NAME}/{blob_path}"
            )

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city_name} from Open-Meteo API: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {city_name}: {e}")

    print("Weather data ingestion completed for all specified cities.")


if __name__ == "__main__":
    ingest_weather_data()
