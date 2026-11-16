
import logging
import shutil

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "weather"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "weather"

LOG_PATH = PROJECT_ROOT / "logs"
LOG_PATH.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "weather_processing.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# CREATE SPARK SESSION
# --------------------------------------------------

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("Weather-Silver-Processing")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# --------------------------------------------------
# IDENTIFY LATEST BRONZE INGESTION RUN
# --------------------------------------------------

def get_latest_bronze_run():

    if not BRONZE_PATH.exists():
        raise FileNotFoundError("Bronze directory does not exist")

    run_directories = sorted(
        [path for path in BRONZE_PATH.iterdir() if path.is_dir()],
        reverse=True
    )

    if not run_directories:
        raise FileNotFoundError("No Bronze ingestion runs found")

    return run_directories[0]


# --------------------------------------------------
# IDENTIFY LATEST SUCCESSFUL SILVER SNAPSHOT
# --------------------------------------------------

def get_latest_silver_run():

    if not SILVER_PATH.exists():
        return None

    run_directories = sorted(
        [
            path for path in SILVER_PATH.iterdir()
            if path.is_dir() and (path / "_SUCCESS").exists()
        ],
        reverse=True
    )

    return run_directories[0] if run_directories else None


# --------------------------------------------------
# READ BRONZE JSON FILES
# --------------------------------------------------

def read_bronze_data(spark, run_path):

    json_files = [
        str(file)
        for file in run_path.glob("*.json")
        if file.name != "failed_locations.json"
    ]

    if not json_files:
        raise FileNotFoundError("No weather JSON files found")

    logger.info(f"Reading {len(json_files)} Bronze JSON files")

    return (
        spark.read
        .option("multiLine", "true")
        .json(json_files)
    )


# --------------------------------------------------
# FLATTEN HOURLY WEATHER ARRAYS
# --------------------------------------------------

def flatten_weather_data(df):

    hourly_data = F.arrays_zip(
        F.col("api_response.hourly.time"),
        F.col("api_response.hourly.temperature_2m"),
        F.col("api_response.hourly.relative_humidity_2m"),
        F.col("api_response.hourly.precipitation"),
        F.col("api_response.hourly.wind_speed_10m"),
        F.col("api_response.hourly.weather_code")
    )

    flattened_df = (
        df
        .select(
            F.col("ingestion_metadata.state").alias("state"),
            F.col("ingestion_metadata.city").alias("city"),
            F.col("ingestion_metadata.latitude").alias("latitude"),
            F.col("ingestion_metadata.longitude").alias("longitude"),
            F.col("ingestion_metadata.ingested_at").alias("ingested_at"),
            F.explode(hourly_data).alias("hourly")
        )
        .select(
            "state",
            "city",
            "latitude",
            "longitude",
            "ingested_at",
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temperature"),
            F.col("hourly.relative_humidity_2m").alias("humidity"),
            F.col("hourly.precipitation").alias("precipitation"),
            F.col("hourly.wind_speed_10m").alias("wind_speed"),
            F.col("hourly.weather_code").alias("weather_code")
        )
    )

    return flattened_df


# --------------------------------------------------
# CLEAN AND TRANSFORM DATA
# --------------------------------------------------

def clean_weather_data(df):

    cleaned_df = (
        df
        .withColumn("timestamp", F.to_timestamp("time"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("temperature", F.col("temperature").cast("double"))
        .withColumn("humidity", F.col("humidity").cast("double"))
        .withColumn("precipitation", F.col("precipitation").cast("double"))
        .withColumn("wind_speed", F.col("wind_speed").cast("double"))
        .withColumn("weather_code", F.col("weather_code").cast("integer"))
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .drop("time")
    )

    cleaned_df = cleaned_df.filter(
        F.col("timestamp").isNotNull()
        & F.col("state").isNotNull()
        & F.col("city").isNotNull()
    )

    return cleaned_df.select(
        "state",
        "city",
        "latitude",
        "longitude",
        "timestamp",
        "date",
        "hour",
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed",
        "weather_code",
        "ingested_at"
    )


# --------------------------------------------------
# INCREMENTAL MERGE AND DEDUPLICATION
# --------------------------------------------------

def merge_incremental_data(spark, new_df, latest_silver_path):

    if latest_silver_path is None:

        logger.info("No previous Silver snapshot found.")
        logger.info("Creating initial Silver dataset.")

        combined_df = new_df

    else:

        logger.info(
            f"Reading previous Silver snapshot: {latest_silver_path.name}"
        )

        existing_df = spark.read.parquet(str(latest_silver_path))

        existing_count = existing_df.count()
        new_count = new_df.count()

        logger.info(f"Existing Silver records: {existing_count}")
        logger.info(f"Incoming Bronze records: {new_count}")

        combined_df = existing_df.unionByName(new_df)

    # Keep the latest ingested version of each city-hour record.

    dedup_window = (
        Window
        .partitionBy("state", "city", "timestamp")
        .orderBy(F.col("ingested_at").desc_nulls_last())
    )

    merged_df = (
        combined_df
        .withColumn(
            "_row_number",
            F.row_number().over(dedup_window)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    return merged_df


# --------------------------------------------------
# WRITE SILVER SNAPSHOT
# --------------------------------------------------

def write_silver_data(df, run_id):

    output_path = SILVER_PATH / run_id

    # Remove an incomplete output from an earlier failed attempt.
    if output_path.exists():

        if (output_path / "_SUCCESS").exists():
            raise FileExistsError(
                f"Silver snapshot already exists: {output_path}"
            )

        shutil.rmtree(output_path)

    (
        df.write
        .mode("errorifexists")
        .partitionBy("state")
        .parquet(str(output_path))
    )

    logger.info(f"Silver snapshot saved to: {output_path}")

    return output_path


# --------------------------------------------------
# MAIN PROCESSING FUNCTION
# --------------------------------------------------

def run_processing():

    logger.info("=" * 60)
    logger.info("INCREMENTAL SILVER PROCESSING STARTED")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:

        bronze_run = get_latest_bronze_run()

        logger.info(f"Latest Bronze run: {bronze_run.name}")

        output_path = SILVER_PATH / bronze_run.name

        # Make reruns of an already completed Bronze batch idempotent.
        if (output_path / "_SUCCESS").exists():

            logger.info(
                "This Bronze run has already been processed. Skipping."
            )

            return {
                "run_id": bronze_run.name,
                "status": "already_processed"
            }

        latest_silver = get_latest_silver_run()

        bronze_df = read_bronze_data(
            spark,
            bronze_run
        )

        flattened_df = flatten_weather_data(bronze_df)

        new_df = clean_weather_data(flattened_df)

        incoming_count = new_df.count()

        logger.info(f"Valid incoming records: {incoming_count}")

        merged_df = merge_incremental_data(
            spark,
            new_df,
            latest_silver
        )

        total_records = merged_df.count()

        city_count = merged_df.select("city").distinct().count()

        logger.info(f"Total Silver records after merge: {total_records}")
        logger.info(f"Unique cities: {city_count}")

        write_silver_data(
            merged_df,
            bronze_run.name
        )

        logger.info("INCREMENTAL SILVER PROCESSING COMPLETED")

        return {
            "run_id": bronze_run.name,
            "status": "success",
            "incoming_records": incoming_count,
            "total_records": total_records,
            "cities": city_count
        }

    finally:

        spark.stop()


# --------------------------------------------------
# EXECUTION ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    result = run_processing()

    print("\nSilver Processing Summary")
    print("-------------------------")
    print(f"Run ID: {result['run_id']}")
    print(f"Status: {result['status']}")

    if result["status"] == "success":
        print(f"Incoming records: {result['incoming_records']}")
        print(f"Total Silver records: {result['total_records']}")
        print(f"Unique cities: {result['cities']}")
