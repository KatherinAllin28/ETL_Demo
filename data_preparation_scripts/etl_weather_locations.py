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
)

# Initialize Spark Session
spark = SparkSession.builder.appName("WeatherLocationsETL").getOrCreate()

# --- Configuration (Update with your actual bucket names) ---
RAW_DATA_BUCKET = "gs://tet-raw-data"  # Ensure this is correct as per your logs
TRUSTED_DATA_BUCKET = "gs://tet-trusted-data"  # Ensure this is correct as per your logs

# Input paths for raw data in GCS
WEATHER_INPUT_PATH = f"{RAW_DATA_BUCKET}/weather_data/2022/*.json"
LOCATIONS_INPUT_PATH = f"{RAW_DATA_BUCKET}/sql_data/locations/*.csv"

# Output path for processed data in GCS (Trusted Zone)
PROCESSED_OUTPUT_PATH = f"{TRUSTED_DATA_BUCKET}/processed_weather_locations_data/"

# --- Explicit Schema for Weather Data ---
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
    # *** IMPORTANT CHANGE HERE ***
    weather_df = (
        spark.read.schema(weather_schema)
        .option("multiline", "true")
        .json(WEATHER_INPUT_PATH)
    )
    # **************************
    print(f"DEBUG: weather_df count: {weather_df.count()}")
    weather_df.printSchema()  # Keep printSchema to verify
    weather_df.show(5)  # Show first 5 rows of weather_df

    print(f"Reading locations data from: {LOCATIONS_INPUT_PATH}")
    locations_df = spark.read.csv(LOCATIONS_INPUT_PATH, header=True, inferSchema=True)
    print(f"DEBUG: locations_df count: {locations_df.count()}")
    locations_df.show(5)  # Show first 5 rows of locations_df

    # --- 2. Prepare Weather Data ---
    print("Preparing weather data...")
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
        .withColumn("weather_date", to_date(col("weather_date_str"), "yyyy-MM-dd"))
    )
    print(f"DEBUG: weather_exploded_df count: {weather_exploded_df.count()}")
    weather_exploded_df.show(5)  # Show first 5 rows of weather_exploded_df

    # --- 3. Prepare Locations Data ---
    print("Preparing locations data...")
    locations_prepared_df = locations_df.select(
        col("location_id"),
        col("city_name"),
        col("country"),
        col("latitude").alias("location_latitude"),
        col("longitude").alias("location_longitude"),
        col("population"),
        col("elevation"),
    )
    print(f"DEBUG: locations_prepared_df count: {locations_prepared_df.count()}")
    locations_prepared_df.show(5)  # Show first 5 rows of locations_prepared_df

    # --- 4. Join DataFrames ---
    print("Joining weather and locations data...")
    joined_df = weather_exploded_df.join(
        locations_prepared_df,
        (weather_exploded_df.latitude == locations_prepared_df.location_latitude)
        & (weather_exploded_df.longitude == locations_prepared_df.location_longitude),
        "inner",
    )
    print(f"DEBUG: joined_df count: {joined_df.count()}")  # THIS IS THE CRUCIAL LINE
    joined_df.show(5)  # Show first 5 rows of joined_df if any

    # --- 5. Select and Write Processed Data to Trusted Zone ---
    final_trusted_df = joined_df.select(
        col("weather_date"),
        col("location_id"),
        col("city_name"),
        col("country"),
        col("max_temp_c"),
        col("precipitation_mm"),
        col("population"),
        col("elevation"),
    ).orderBy("weather_date", "city_name")

    print(f"Writing processed data to: {PROCESSED_OUTPUT_PATH}")
    final_trusted_df.write.mode("overwrite").parquet(PROCESSED_OUTPUT_PATH)

    print("ETL job completed successfully!")

    spark.stop()


if __name__ == "__main__":
    run_etl()
