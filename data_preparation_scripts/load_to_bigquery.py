from google.cloud import bigquery
import os


def load_parquet_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    source_uris: str,
    location: str = "US",  # Or your desired BigQuery location
):
    """
    Loads Parquet files from Google Cloud Storage into a BigQuery table.

    Args:
        project_id (str): Your Google Cloud Project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID to load data into.
        source_uris (str): Google Cloud Storage URI(s) of the Parquet files.
                           Can include wildcards (e.g., 'gs://bucket/path/*.parquet').
        location (str): BigQuery dataset location (e.g., 'US', 'EU', 'southamerica-east1').
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # Automatically detect schema from Parquet metadata
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table if it exists
    )

    print(
        f"Starting BigQuery load job from {source_uris} to {project_id}.{dataset_id}.{table_id}..."
    )

    load_job = client.load_table_from_uri(
        source_uris,
        table_ref,
        job_config=job_config,
        location=location,
    )

    load_job.result()  # Waits for the job to complete

    print(f"Load job completed successfully. Rows loaded: {load_job.output_rows}")
    print(f"Table {project_id}.{dataset_id}.{table_id} updated.")


if __name__ == "__main__":
    # --- Configuration ---
    # Replace with your actual project ID and desired BigQuery details
    YOUR_PROJECT_ID = "tet-p3-2025"  # e.g., "my-gcp-project-12345"
    BIGQUERY_DATASET_ID = "tet_analytics"
    BIGQUERY_TABLE_ID = "weather_locations_data"
    GCS_PROCESSED_DATA_URI = (
        "gs://tet-trusted-data/processed_weather_locations_data/*.parquet"
    )
    BIGQUERY_LOCATION = (
        "us-east1"  # Or your specific region (e.g., 'southamerica-east1')
    )

    # Ensure your environment is authenticated (e.g., via `gcloud auth application-default login`
    # or by setting GOOGLE_APPLICATION_CREDENTIALS)

    try:
        load_parquet_to_bigquery(
            project_id=YOUR_PROJECT_ID,
            dataset_id=BIGQUERY_DATASET_ID,
            table_id=BIGQUERY_TABLE_ID,
            source_uris=GCS_PROCESSED_DATA_URI,
            location=BIGQUERY_LOCATION,
        )
    except Exception as e:
        print(f"An error occurred: {e}")
