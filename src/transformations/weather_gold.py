
import logging
import shutil

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "weather"

GOLD_PATH = PROJECT_ROOT / "data" / "gold"

DAILY_GOLD_PATH = GOLD_PATH / "daily_weather"

INSIGHTS_GOLD_PATH = GOLD_PATH / "city_weather_insights"

TRENDS_GOLD_PATH = GOLD_PATH / "weather_trends"


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

LOG_PATH = PROJECT_ROOT / "logs"

LOG_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_PATH / "weather_gold.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ],
    force=True
)

logger = logging.getLogger(__name__)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("EnvironmentalWeatherGold")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# GET LATEST SUCCESSFUL SILVER RUN
# ============================================================

def get_latest_silver_run():

    if not SILVER_PATH.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_PATH}"
        )

    successful_runs = [
        folder
        for folder in SILVER_PATH.iterdir()
        if folder.is_dir()
        and (folder / "_SUCCESS").exists()
    ]

    if not successful_runs:
        raise FileNotFoundError(
            "No successful Silver processing run found."
        )

    latest_run = max(
        successful_runs,
        key=lambda folder: folder.name
    )

    logger.info(
        "Latest Silver run: %s",
        latest_run.name
    )

    return latest_run.name


# ============================================================
# READ SILVER DATA
# ============================================================

def read_silver_data(spark, silver_run_id):

    silver_run_path = SILVER_PATH / silver_run_id

    logger.info(
        "Reading Silver data from: %s",
        silver_run_path
    )

    if not (silver_run_path / "_SUCCESS").exists():
        raise RuntimeError(
            "Silver dataset is incomplete or missing _SUCCESS."
        )

    df = spark.read.parquet(str(silver_run_path))

    required_columns = [
        "state",
        "city",
        "date",
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required Silver columns: {missing_columns}"
        )

    logger.info("Silver data loaded successfully.")

    return df


# ============================================================
# DAILY WEATHER AGGREGATION
# ============================================================

def create_daily_weather_summary(df):

    logger.info(
        "Creating daily weather aggregations."
    )

    daily_df = (
        df
        .groupBy(
            "state",
            "city",
            "date"
        )
        .agg(
            F.round(
                F.avg("temperature"), 2
            ).alias("avg_temperature"),

            F.min(
                "temperature"
            ).alias("min_temperature"),

            F.max(
                "temperature"
            ).alias("max_temperature"),

            F.round(
                F.avg("humidity"), 2
            ).alias("avg_humidity"),

            F.round(
                F.avg("wind_speed"), 2
            ).alias("avg_wind_speed"),

            F.round(
                F.sum("precipitation"), 2
            ).alias("total_precipitation"),

            F.count("*").alias("hourly_records")
        )
        .withColumn(
            "rainy_day",
            F.col("total_precipitation") > 0
        )
    )

    logger.info(
        "Daily weather aggregation completed."
    )

    return daily_df


# ============================================================
# CITY WEATHER INSIGHTS
# ============================================================

def create_city_weather_insights(daily_df):

    logger.info(
        "Creating city weather insights."
    )

    insights_df = (
        daily_df

        # Temperature classification
        .withColumn(
            "temperature_category",
            F.when(
                F.col("avg_temperature") < 15,
                "Cool"
            )
            .when(
                F.col("avg_temperature") <= 25,
                "Moderate"
            )
            .when(
                F.col("avg_temperature") <= 35,
                "Warm"
            )
            .otherwise("Hot")
        )

        # Humidity classification
        .withColumn(
            "humidity_category",
            F.when(
                F.col("avg_humidity") < 30,
                "Low"
            )
            .when(
                F.col("avg_humidity") <= 60,
                "Moderate"
            )
            .otherwise("High")
        )

        # Wind classification
        .withColumn(
            "wind_category",
            F.when(
                F.col("avg_wind_speed") < 10,
                "Calm"
            )
            .when(
                F.col("avg_wind_speed") <= 25,
                "Moderate"
            )
            .otherwise("Strong")
        )
    )

    # Ranking windows by date
    temperature_window = (
        Window
        .partitionBy("date")
        .orderBy(
            F.col("avg_temperature").desc()
        )
    )

    rainfall_window = (
        Window
        .partitionBy("date")
        .orderBy(
            F.col("total_precipitation").desc()
        )
    )

    wind_window = (
        Window
        .partitionBy("date")
        .orderBy(
            F.col("avg_wind_speed").desc()
        )
    )

    insights_df = (
        insights_df

        .withColumn(
            "temperature_rank",
            F.dense_rank().over(
                temperature_window
            )
        )

        .withColumn(
            "rainfall_rank",
            F.dense_rank().over(
                rainfall_window
            )
        )

        .withColumn(
            "wind_speed_rank",
            F.dense_rank().over(
                wind_window
            )
        )

        # Overall weather classification
        .withColumn(
            "weather_category",

            F.when(
                (F.col("rainy_day") == True)
                & (F.col("avg_temperature") >= 30),
                "Hot and Rainy"
            )

            .when(
                F.col("rainy_day") == True,
                "Rainy"
            )

            .when(
                F.col("avg_temperature") >= 30,
                "Hot and Dry"
            )

            .otherwise("Normal")
        )
    )

    logger.info(
        "City weather insights created successfully."
    )

    return insights_df


# ============================================================
# WEATHER TRENDS
# ============================================================

def create_weather_trends(daily_df):

    logger.info(
        "Creating day-over-day weather trends."
    )

    trend_window = (
        Window
        .partitionBy(
            "state",
            "city"
        )
        .orderBy("date")
    )

    trends_df = (
        daily_df

        # Previous day's values
        .withColumn(
            "previous_date",
            F.lag("date").over(trend_window)
        )

        .withColumn(
            "previous_temperature",
            F.lag("avg_temperature").over(
                trend_window
            )
        )

        .withColumn(
            "previous_precipitation",
            F.lag("total_precipitation").over(
                trend_window
            )
        )

        .withColumn(
            "previous_wind_speed",
            F.lag("avg_wind_speed").over(
                trend_window
            )
        )

        # Number of days between available records
        .withColumn(
            "days_between_records",
            F.datediff(
                F.col("date"),
                F.col("previous_date")
            )
        )

        # Temperature change
        .withColumn(
            "temperature_change",
            F.round(
                F.col("avg_temperature")
                - F.col("previous_temperature"),
                2
            )
        )

        # Precipitation change
        .withColumn(
            "precipitation_change",
            F.round(
                F.col("total_precipitation")
                - F.col("previous_precipitation"),
                2
            )
        )

        # Wind speed change
        .withColumn(
            "wind_speed_change",
            F.round(
                F.col("avg_wind_speed")
                - F.col("previous_wind_speed"),
                2
            )
        )

        # Temperature trend classification
        .withColumn(
            "temperature_trend",

            F.when(
                F.col("previous_date").isNull(),
                "Insufficient History"
            )

            .when(
                F.col("temperature_change") > 0.5,
                "Warmer"
            )

            .when(
                F.col("temperature_change") < -0.5,
                "Cooler"
            )

            .otherwise("Stable")
        )

        # Rainfall trend classification
        .withColumn(
            "rainfall_trend",

            F.when(
                F.col("previous_date").isNull(),
                "Insufficient History"
            )

            .when(
                F.col("precipitation_change") > 0,
                "Increasing"
            )

            .when(
                F.col("precipitation_change") < 0,
                "Decreasing"
            )

            .otherwise("Stable")
        )

        # Wind speed trend classification
        .withColumn(
            "wind_speed_trend",

            F.when(
                F.col("previous_date").isNull(),
                "Insufficient History"
            )

            .when(
                F.col("wind_speed_change") > 0,
                "Increasing"
            )

            .when(
                F.col("wind_speed_change") < 0,
                "Decreasing"
            )

            .otherwise("Stable")
        )

        # Keep only analytical output columns
        .select(
            "state",
            "city",
            "date",
            "previous_date",
            "days_between_records",
            "temperature_change",
            "temperature_trend",
            "precipitation_change",
            "rainfall_trend",
            "wind_speed_change",
            "wind_speed_trend"
        )
    )

    logger.info(
        "Weather trends created successfully."
    )

    return trends_df


# ============================================================
# WRITE GOLD DATA
# ============================================================

def write_gold_data(df, output_path):

    logger.info(
        "Writing Gold dataset: %s",
        output_path
    )

    (
        df.write
        .mode("errorifexists")
        .partitionBy("state")
        .parquet(str(output_path))
    )

    logger.info(
        "Gold dataset written successfully."
    )


# ============================================================
# GET OR CREATE GOLD DATASET
# ============================================================

def get_or_create_gold_dataset(
    df,
    output_path,
    dataset_name
):

    output_path = Path(output_path)

    # Reuse an already completed dataset
    if output_path.exists():

        if (output_path / "_SUCCESS").exists():

            logger.info(
                "%s already exists. Reusing dataset: %s",
                dataset_name,
                output_path
            )

            return

        raise RuntimeError(
            f"Incomplete {dataset_name} dataset exists: "
            f"{output_path}. Remove it manually after "
            f"checking the failed run."
        )

    write_gold_data(
        df,
        output_path
    )


# ============================================================
# READ GOLD DATA
# ============================================================

def read_gold_data(spark, output_path):

    if not (Path(output_path) / "_SUCCESS").exists():
        raise RuntimeError(
            f"Gold dataset is incomplete: {output_path}"
        )

    return spark.read.parquet(
        str(output_path)
    )


# ============================================================
# MAIN GOLD TRANSFORMATION
# ============================================================

def run_gold_transformation():

    spark = None

    try:

        logger.info("=" * 60)
        logger.info("GOLD TRANSFORMATION STARTED")
        logger.info("=" * 60)

        # Create Spark session
        spark = create_spark_session()

        # Identify latest successful Silver snapshot
        silver_run_id = get_latest_silver_run()

        # Read Silver data
        silver_df = read_silver_data(
            spark,
            silver_run_id
        )

        # ----------------------------------------------------
        # 1. DAILY WEATHER GOLD
        # ----------------------------------------------------

        logger.info(
            "Processing Daily Weather Gold dataset."
        )

        daily_df = create_daily_weather_summary(
            silver_df
        )

        daily_output = (
            DAILY_GOLD_PATH / silver_run_id
        )

        get_or_create_gold_dataset(
            daily_df,
            daily_output,
            "Daily Weather Gold"
        )

        daily_gold_df = read_gold_data(
            spark,
            daily_output
        )

        daily_count = daily_gold_df.count()

        # ----------------------------------------------------
        # 2. CITY WEATHER INSIGHTS GOLD
        # ----------------------------------------------------

        logger.info(
            "Processing City Weather Insights dataset."
        )

        insights_df = create_city_weather_insights(
            daily_gold_df
        )

        insights_output = (
            INSIGHTS_GOLD_PATH / silver_run_id
        )

        get_or_create_gold_dataset(
            insights_df,
            insights_output,
            "City Weather Insights Gold"
        )

        insights_gold_df = read_gold_data(
            spark,
            insights_output
        )

        insights_count = insights_gold_df.count()

        # ----------------------------------------------------
        # 3. WEATHER TRENDS GOLD
        # ----------------------------------------------------

        logger.info(
            "Processing Weather Trends dataset."
        )

        trends_df = create_weather_trends(
            daily_gold_df
        )

        trends_output = (
            TRENDS_GOLD_PATH / silver_run_id
        )

        get_or_create_gold_dataset(
            trends_df,
            trends_output,
            "Weather Trends Gold"
        )

        trends_gold_df = read_gold_data(
            spark,
            trends_output
        )

        trends_count = trends_gold_df.count()

        # ----------------------------------------------------
        # FINAL SUMMARY
        # ----------------------------------------------------

        logger.info("=" * 60)
        logger.info("GOLD TRANSFORMATION SUMMARY")
        logger.info("=" * 60)

        logger.info(
            "Silver Run ID: %s",
            silver_run_id
        )

        logger.info(
            "Daily Gold Records: %s",
            daily_count
        )

        logger.info(
            "City Weather Insights Records: %s",
            insights_count
        )

        logger.info(
            "Weather Trends Records: %s",
            trends_count
        )

        # ----------------------------------------------------
        # DATA PREVIEWS
        # ----------------------------------------------------

        print("\nDaily Weather Preview:")

        daily_gold_df.show(
            10,
            truncate=False
        )

        print("\nCity Weather Insights Preview:")

        insights_gold_df.show(
            10,
            truncate=False
        )

        print("\nWeather Trends Preview:")

        trends_gold_df.show(
            20,
            truncate=False
        )

        logger.info("=" * 60)
        logger.info("GOLD TRANSFORMATION COMPLETED")
        logger.info("=" * 60)

    except Exception:

        logger.exception(
            "Gold transformation failed."
        )

        raise

    finally:

        if spark is not None:

            logger.info(
                "Stopping Spark session."
            )

            spark.stop()


# ============================================================
# EXECUTION ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_gold_transformation()
