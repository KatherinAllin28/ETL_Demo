from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode, arrays_zip, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    ArrayType,
    DateType,
    IntegerType,
)  # Import necessary types

# Initialize Spark Session
spark = SparkSession.builder.appName("WeatherLocationsETL").getOrCreate()

# --- Configuration (Update with your actual bucket names) ---
RAW_DATA_BUCKET = "gs://tet-raw-data"
TRUSTED_DATA_BUCKET = "gs://tet-trusted-data"

# Input paths for raw data in GCS
# Adjust WEATHER_INPUT_PATH if your blob name pattern is different (e.g., just "*.json")
WEATHER_INPUT_PATH = f"{RAW_DATA_BUCKET}/weather_data/2022/*.json"
LOCATIONS_INPUT_PATH = f"{RAW_DATA_BUCKET}/sql_data/locations/*.csv"

# Output path for processed data in GCS (Trusted Zone)
PROCESSED_OUTPUT_PATH = f"{TRUSTED_DATA_BUCKET}/processed_weather_locations_data/"

# --- Explicit Schema for Weather Data ---
# This schema matches the structure of the Open-Meteo API response for daily data
weather_schema = StructType(
    [
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("generationtime_ms", DoubleType(), True),
        StructField("utc_offset_seconds", IntegerType(), True),
        StructField("timezone", StringType(), True),
        StructField("timezone_abbreviation", StringType(), True),
        StructField("elevation", DoubleType(), True),
        StructField(
            "daily_units",
            StructType(
                [
                    StructField("time", StringType(), True),
                    StructField("temperature_2m_max", StringType(), True),
                    StructField("precipitation_sum", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "daily",
            StructType(
                [
                    StructField("time", ArrayType(StringType()), True),
                    StructField("temperature_2m_max", ArrayType(DoubleType()), True),
                    StructField("precipitation_sum", ArrayType(DoubleType()), True),
                ]
            ),
            True,
        ),
    ]
)


def run_etl():
    print(f"Starting ETL job...")

    # --- 1. Read Raw Data ---
    print(f"Reading weather data from: {WEATHER_INPUT_PATH}")
    # Read weather data with explicit schema
    weather_df = spark.read.schema(weather_schema).json(WEATHER_INPUT_PATH)
    # weather_df.printSchema() # Uncomment to see the schema for debugging

    print(f"Reading locations data from: {LOCATIONS_INPUT_PATH}")
    # Read locations data (CSV format from Cloud SQL extraction)
    locations_df = spark.read.csv(LOCATIONS_INPUT_PATH, header=True, inferSchema=True)
    # locations_df.printSchema() # Uncomment to see the schema for debugging

    # --- 2. Prepare Weather Data ---
    print("Preparing weather data...")
    # Open-Meteo's JSON has 'daily' as a struct containing arrays.
    # We need to flatten it so each row represents one day's weather data.
    # The 'arrays_zip' function pairs elements from corresponding arrays.
    # 'explode' then creates a new row for each element in the zipped array.
    weather_exploded_df = (
        weather_df.select(
            col("latitude"),
            col("longitude"),
            explode(
                arrays_zip(
                    col("daily.time"),
                    col("daily.temperature_2m_max"),
                    col("daily.precipitation_sum"),
                )
            ).alias("daily_record"),
        )
        .select(
            col("latitude"),
            col("longitude"),
            col("daily_record.time").alias("weather_date_str"),
            col("daily_record.temperature_2m_max").alias("max_temp_c"),
            col("daily_record.precipitation_sum").alias("precipitation_mm"),
        )
        .withColumn(
            "weather_date",
            to_date(
                col("weather_date_str"), "yyyy-MM-dd"
            ),  # Convert date string to Date type
        )
    )

    # --- 3. Prepare Locations Data ---
    print("Preparing locations data...")
    # Select relevant columns and potentially rename them for clarity or to avoid conflicts during join.
    locations_prepared_df = locations_df.select(
        col("location_id"),
        col("city_name"),
        col("country"),
        col("latitude").alias(
            "location_latitude"
        ),  # Rename to differentiate from weather_df's latitude
        col("longitude").alias(
            "location_longitude"
        ),  # Rename to differentiate from weather_df's longitude
        col("population"),
        col("elevation"),
        # Add any other columns you want from your locations table
    )

    # --- 4. Join DataFrames ---
    print("Joining weather and locations data...")
    # Join on latitude and longitude to link weather data to specific locations.
    joined_df = weather_exploded_df.join(
        locations_prepared_df,
        (weather_exploded_df.latitude == locations_prepared_df.location_latitude)
        & (weather_exploded_df.longitude == locations_prepared_df.location_longitude),
        "inner",  # Use "inner" join to get only matching records
    )

    # Select and reorder final columns for the trusted dataset
    final_trusted_df = joined_df.select(
        col("weather_date"),
        col("location_id"),
        col("city_name"),
        col("country"),
        col("max_temp_c"),
        col("precipitation_mm"),
        col("population"),
        col("elevation"),
        # Include any other columns you need in your final trusted dataset
    ).orderBy("weather_date", "city_name")  # Optional: order for consistency

    # --- 5. Write Processed Data to Trusted Zone ---
    print(f"Writing processed data to: {PROCESSED_OUTPUT_PATH}")
    final_trusted_df.write.mode("overwrite").parquet(PROCESSED_OUTPUT_PATH)

    print("ETL job completed successfully!")

    spark.stop()


if __name__ == "__main__":
    run_etl()
