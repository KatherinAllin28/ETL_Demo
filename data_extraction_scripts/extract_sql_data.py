import mysql.connector  # Or import psycopg2 for PostgreSQL
from google.cloud import storage
import os
import csv
from io import StringIO
from datetime import datetime  # Added for dynamic file naming

# --- Configuration ---
# Replace with your actual project ID and bucket name
PROJECT_ID = "tet-p3-2025"
RAW_DATA_BUCKET_NAME = "tet-raw-data"  # Use your actual raw data bucket name

# Cloud SQL Connection Details (replace with your instance details)
# IMPORTANT: Use the Public IP of your Cloud SQL instance.
# Ensure this IP is allowed in your Cloud SQL authorized networks.
DB_HOST = "34.139.223.118"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "tet-proyecto3-data"
DB_TABLE = "locations"  # The table you created in Cloud SQL (e.g., 'locations')

# Destination blob name in GCS for the extracted SQL data
# Example: sql_data/locations/locations_data_YYYYMMDD_HHMMSS.csv
GCS_DESTINATION_BLOB_SQL = (
    f"sql_data/{DB_TABLE}/locations_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
)


def upload_stringio_to_gcs(
    data_string_io, bucket_name, destination_blob_name, project_id
):
    """Uploads data from a StringIO object to GCS."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Get the string value from StringIO
    csv_content_string = data_string_io.getvalue()

    # Upload the string content, which handles the encoding to bytes
    blob.upload_from_string(csv_content_string, content_type="text/csv")

    print(
        f"Data from {DB_TABLE} uploaded to gs://{bucket_name}/{destination_blob_name}"
    )


def extract_from_cloud_sql_and_upload():
    """
    Connects to Cloud SQL, extracts data from a table, and uploads it to GCS.
    """
    print(f"Extracting data from Cloud SQL table: {DB_TABLE}...")
    connection = None
    try:
        # Establish connection to Cloud SQL
        # Use mysql.connector.connect for MySQL
        # Use psycopg2.connect for PostgreSQL (you'd change this line if using Postgres)
        connection = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        cursor = connection.cursor()

        # Fetch all data from the specified table
        cursor.execute(f"SELECT * FROM {DB_TABLE}")
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]  # Get column headers

        # Write data to an in-memory CSV file (StringIO)
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(column_names)  # Write header row
        csv_writer.writerows(rows)  # Write all data rows

        # Upload the in-memory CSV content to GCS
        upload_stringio_to_gcs(
            output, RAW_DATA_BUCKET_NAME, GCS_DESTINATION_BLOB_SQL, PROJECT_ID
        )

    except mysql.connector.Error as err:  # Or psycopg2.Error for PostgreSQL
        print(f"Error connecting to or querying MySQL: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    # Set GOOGLE_APPLICATION_CREDENTIALS if running locally and not in Cloud Shell
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/service-account-key.json"
    extract_from_cloud_sql_and_upload()
