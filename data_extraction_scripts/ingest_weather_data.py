import json
from google.cloud import storage
import requests
import os
from datetime import datetime

# --- Configuration ---
# Replace with your actual project ID and bucket name
PROJECT_ID = "tet-p3-2025"
RAW_DATA_BUCKET_NAME = "tet-raw-data"  # Use your actual raw data bucket name

# Open-Meteo API Details (from project description)
# You can customize these parameters for specific dates or locations
OPEN_METEO_API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 6.2214413  # Example: Latitude for Medellin
LONGITUDE = -75.47659  # Example: Longitude for Medellin
START_DATE = "2022-01-01"
END_DATE = "2022-12-31"
DAILY_VARIABLES = "temperature_2m_max,precipitation_sum"
TIMEZONE = "America/Bogota"


def upload_string_to_gcs(
    data_string,
    bucket_name,
    destination_blob_name,
    project_id,
    content_type="application/json",
):
    """Uploads a string (e.g., JSON data) to the Google Cloud Storage bucket."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data_string, content_type=content_type)

    print(f"Data uploaded to gs://{bucket_name}/{destination_blob_name}")


def ingest_online_weather_data_via_api():
    """
    Fetches online weather data from Open-Meteo API and uploads it to GCS.
    """
    print(
        f"Fetching weather data from Open-Meteo API for {START_DATE} to {END_DATE}..."
    )

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": DAILY_VARIABLES,
        "timezone": TIMEZONE,
    }

    try:
        response = requests.get(OPEN_METEO_API_BASE_URL, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        weather_data = response.json()
        print("Weather data fetched successfully from Open-Meteo API.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Open-Meteo API: {e}")
        return

    # Determine destination blob name in GCS dynamically
    # Example: weather_data/YYYY/MM/DD/open_meteo_YYYYMMDD_HHMMSS.json
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    year = datetime.strptime(START_DATE, "%Y-%m-%d").year
    destination_blob = f"weather_data/{year}/open_meteo_data_{current_datetime}.json"

    # Upload the JSON string to raw-data-bucket
    json_data_string = json.dumps(weather_data, indent=2)
    upload_string_to_gcs(
        json_data_string, RAW_DATA_BUCKET_NAME, destination_blob, PROJECT_ID
    )


if __name__ == "__main__":
    # Set GOOGLE_APPLICATION_CREDENTIALS if running locally and not in Cloud Shell
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/service-account-key.json"
    ingest_online_weather_data_via_api()
