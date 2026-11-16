
import logging
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = PROJECT_ROOT / "data" / "gold"
POWERBI_PATH = PROJECT_ROOT / "data" / "powerbi"

DATASETS = {
    "daily_weather": GOLD_PATH / "daily_weather",
    "city_weather_insights": GOLD_PATH / "city_weather_insights",
    "weather_trends": GOLD_PATH / "weather_trends",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

def get_latest_gold_run(dataset_path: Path) -> Path:
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Gold dataset not found: {dataset_path}"
        )

    completed_runs = [
        folder
        for folder in dataset_path.iterdir()
        if folder.is_dir() and (folder / "_SUCCESS").exists()
    ]

    if not completed_runs:
        raise FileNotFoundError(
            f"No completed Gold runs found in {dataset_path}"
        )

    return sorted(completed_runs, key=lambda x: x.name)[-1]

def export_dataset(spark, dataset_name: str, dataset_path: Path):
    logger.info(f"Exporting dataset: {dataset_name}")

    latest_run = get_latest_gold_run(dataset_path)

    logger.info(f"Using Gold run: {latest_run.name}")

    df = spark.read.parquet(str(latest_run))

    output_file = POWERBI_PATH / f"{dataset_name}.csv"
    temp_folder = POWERBI_PATH / f"_temp_{dataset_name}"

    if temp_folder.exists():
        shutil.rmtree(temp_folder)

    if output_file.exists():
        output_file.unlink()

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(temp_folder))
    )

    csv_files = list(temp_folder.glob("part-*.csv"))

    if not csv_files:
        raise RuntimeError(
            f"CSV export failed for {dataset_name}"
        )

    shutil.move(str(csv_files[0]), str(output_file))
    shutil.rmtree(temp_folder)

    logger.info(
        f"Export completed: {output_file} | Records: {df.count()}"
    )

def main():
    POWERBI_PATH.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("GoldToPowerBIExport")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        for dataset_name, dataset_path in DATASETS.items():
            export_dataset(spark, dataset_name, dataset_path)

        logger.info("=" * 60)
        logger.info("ALL POWER BI EXPORTS COMPLETED")
        logger.info("=" * 60)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()