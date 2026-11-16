
import logging
import subprocess
import sys

from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PYTHON_EXECUTABLE = sys.executable

MAX_RETRIES = 2
RETRY_DELAY = 30


# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# --------------------------------------------------
# COMMON TASK EXECUTOR
# --------------------------------------------------

@task(
    retries=MAX_RETRIES,
    retry_delay_seconds=RETRY_DELAY,
    log_prints=True
)
def run_script(module_name: str):

    logger = get_run_logger()

    logger.info("=" * 60)
    logger.info(f"STARTING TASK: {module_name}")
    logger.info("=" * 60)

    command = [
        PYTHON_EXECUTABLE,
        "-m",
        module_name
    ]

    try:

        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True
        )

        if result.stdout:
            logger.info(result.stdout)

        if result.stderr:
            logger.info(result.stderr)

        logger.info(f"COMPLETED TASK: {module_name}")

        return {
            "module": module_name,
            "status": "success"
        }

    except subprocess.CalledProcessError as error:

        logger.error(
            f"FAILED TASK: {module_name}"
        )

        if error.stdout:
            logger.error(error.stdout)

        if error.stderr:
            logger.error(error.stderr)

        raise RuntimeError(
            f"Execution failed for {module_name}"
        ) from error


# --------------------------------------------------
# PREFECT PIPELINE
# --------------------------------------------------

@flow(
    name="Environmental Data Intelligence Pipeline",
    log_prints=True
)
def environmental_data_pipeline():

    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info("ENVIRONMENTAL DATA INTELLIGENCE PIPELINE STARTED")
    logger.info("=" * 70)

    # ----------------------------------------------
    # STAGE 1: WEATHER API → BRONZE
    # ----------------------------------------------

    bronze_task = run_script.submit(
        "src.ingestion.weather_api"
    )

    # ----------------------------------------------
    # STAGE 2: BRONZE → SILVER
    # ----------------------------------------------

    silver_task = run_script.submit(
        "src.processing.weather_cleaning",
        wait_for=[bronze_task]
    )

    # ----------------------------------------------
    # STAGE 3: SILVER → GOLD
    # ----------------------------------------------

    gold_task = run_script.submit(
        "src.transformations.weather_gold",
        wait_for=[silver_task]
    )

    # Wait for all stages and propagate failures
    bronze_result = bronze_task.result()
    silver_result = silver_task.result()
    gold_result = gold_task.result()

    logger.info("=" * 70)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"Bronze: {bronze_result['status']}")
    logger.info(f"Silver: {silver_result['status']}")
    logger.info(f"Gold:   {gold_result['status']}")

    logger.info("=" * 70)
    logger.info("ENVIRONMENTAL DATA INTELLIGENCE PIPELINE COMPLETED")
    logger.info("=" * 70)


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    environmental_data_pipeline()
