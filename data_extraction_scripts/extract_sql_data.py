import pandas as pd
import numpy as np  # Import numpy for advanced rounding
import mysql.connector  # Or whatever database connector you are using (e.g., pymysql)
# Make sure you have 'gcsfs' installed if you are writing directly to 'gs://' paths with pandas

# --- Database connection details ---
# Make sure to fill in your actual database credentials and details
DB_HOST = "34.139.223.118"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "tet-proyecto3-data"
DB_TABLE = "locations"  # Assuming this is your locations table

# --- GCS path for output CSV ---
OUTPUT_GCS_PATH = "gs://tet-raw-data/sql_data/locations/locations.csv"


# Function to round a coordinate to the nearest 0.125
def round_to_nearest_0125(coord):
    """Rounds a floating-point number to the nearest 0.125."""
    return np.round(coord / 0.125) * 0.125


def extract_and_transform_locations():
    print("Starting location data extraction and transformation...")
    conn = None  # Initialize conn to None
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        query = f"SELECT location_id, city_name, country, latitude, longitude, population, elevation, average_tourism_visits_per_year FROM {DB_TABLE}"
        locations_df = pd.read_sql(query, conn)

        # --- NEW: Transformation to round latitude and longitude to match weather API's grid ---
        print("Rounding latitude and longitude to nearest 0.125...")
        locations_df["latitude"] = locations_df["latitude"].apply(round_to_nearest_0125)
        locations_df["longitude"] = locations_df["longitude"].apply(
            round_to_nearest_0125
        )
        # --- END NEW ---

        # You can print a sample to verify the rounding if you want
        # print("Sample of locations_df after rounding:")
        # print(locations_df[['city_name', 'latitude', 'longitude']].head())

        # Write to CSV in GCS
        # Ensure you have 'gcsfs' installed ('pip install gcsfs') for pandas to write to gs://
        locations_df.to_csv(OUTPUT_GCS_PATH, index=False)
        print(f"Successfully extracted and transformed data to {OUTPUT_GCS_PATH}")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    extract_and_transform_locations()
