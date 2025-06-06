from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode, arrays_zip, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    ArrayType,
    DateType,
    IntegerType,
)
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder.appName("WeatherLocationsETL").getOrCreate()

# --- Configuration (Update with your actual bucket names) ---
RAW_DATA_BUCKET = "gs://tet-raw-data"
TRUSTED_DATA_BUCKET = "gs://tet-trusted-data"

# Input paths for raw data in GCS
# Original path: WEATHER_INPUT_PATH = f"{RAW_DATA_BUCKET}/weather_data/2022/*.json"
# For debugging: Use a specific known good file and a different variable name
WEATHER_INPUT_PATH_DEBUG = (
    f"{RAW_DATA_BUCKET}/weather_data/2022/*.json"  # <--- DEBUGGING CHANGE HERE
)
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


# --- Define UDF for rounding to nearest 0.125 ---
def round_to_nearest_0125_py(coord):
    if coord is None:
        return None
    return float(np.round(coord / 0.125) * 0.125)


# Register the UDF
round_to_nearest_0125_udf = udf(round_to_nearest_0125_py, DoubleType())


# --- ETL Logic ---
def run_etl():
    # --- 1. Read Raw Data from GCS ---
    print(f"Reading weather data from: {WEATHER_INPUT_PATH_DEBUG}")
    # Use the explicit schema and multiLine option to read weather data
    weather_df = (
        spark.read.schema(weather_schema)
        .option("multiLine", True)
        .json(WEATHER_INPUT_PATH_DEBUG)
    )  # <--- DEBUGGING CHANGE HERE
    print(f"DEBUG: weather_df count: {weather_df.count()}")
    weather_df.printSchema()
    weather_df.show(5, truncate=False)

    # Read locations data (no change here)
    print(f"Reading locations data from: {LOCATIONS_INPUT_PATH}")
    locations_df = spark.read.option("header", "true").csv(LOCATIONS_INPUT_PATH)
    print(f"DEBUG: locations_df count: {locations_df.count()}")
    locations_df.printSchema()
    locations_df.show(5, truncate=False)

    # --- 2. Prepare Weather Data ---
    print("Preparing weather data...")
    # This section remains the same, but it depends on weather_df having data
    weather_exploded_df = (
        weather_df.select(
            round_to_nearest_0125_udf(col("latitude")).alias("latitude"),
            round_to_nearest_0125_udf(col("longitude")).alias("longitude"),
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
        .withColumn("weather_date", to_date(col("weather_date_str")))
    )

    print(f"DEBUG: weather_exploded_df count: {weather_exploded_df.count()}")
    weather_exploded_df.printSchema()
    weather_exploded_df.show(5, truncate=False)

    # --- 3. Prepare Locations Data ---
    print("Preparing locations data...")
    # This section remains the same
    locations_prepared_df = locations_df.select(
        col("location_id").cast("integer"),
        col("city_name"),
        col("country"),
        round_to_nearest_0125_udf(col("latitude").cast("double")).alias(
            "location_latitude"
        ),
        round_to_nearest_0125_udf(col("longitude").cast("double")).alias(
            "location_longitude"
        ),
        col("population").cast("integer"),
        col("elevation").cast("double"),
    )
    print(f"DEBUG: locations_prepared_df count: {locations_prepared_df.count()}")
    locations_prepared_df.printSchema()
    locations_prepared_df.show(5, truncate=False)

    # --- 4. Join DataFrames ---
    print("Joining weather and locations data...")
    # This section remains the same
    joined_df = weather_exploded_df.join(
        locations_prepared_df,
        (weather_exploded_df.latitude == locations_prepared_df.location_latitude)
        & (weather_exploded_df.longitude == locations_prepared_df.location_longitude),
        "inner",
    )
    print(f"DEBUG: joined_df count: {joined_df.count()}")
    joined_df.printSchema()
    joined_df.show(5, truncate=False)

    # --- 5. Select and Write Processed Data to Trusted Zone ---
    print(f"Writing processed data to: {PROCESSED_OUTPUT_PATH}")
    # This section remains the same
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

    final_trusted_df.write.mode("overwrite").parquet(PROCESSED_OUTPUT_PATH)
    print("ETL job completed successfully!")

    spark.stop()


if __name__ == "__main__":
    run_etl()
