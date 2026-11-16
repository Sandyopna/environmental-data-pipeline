
import json
import logging
import re
import time

from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.cities import CITIES


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "weather"
LOG_PATH = PROJECT_ROOT / "logs"

API_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code"
]

# Fetch yesterday's data and today's forecast
PAST_DAYS = 1
FORECAST_DAYS = 1

REQUEST_TIMEOUT = 30


# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------

LOG_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "weather_ingestion.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# CREATE HTTP SESSION WITH RETRY
# --------------------------------------------------

def create_session():

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount("https://", adapter)

    return session


# --------------------------------------------------
# FETCH WEATHER DATA FROM API
# --------------------------------------------------

def fetch_weather(session, city):

    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "past_days": PAST_DAYS,
        "forecast_days": FORECAST_DAYS,
        "timezone": "auto"
    }

    response = session.get(
        API_URL,
        params=params,
        timeout=REQUEST_TIMEOUT
    )

    response.raise_for_status()

    weather_data = response.json()

    if weather_data.get("error"):
        raise ValueError(weather_data.get("reason", "API returned an error"))

    if not weather_data.get("hourly"):
        raise ValueError("Hourly weather data missing from API response")

    return weather_data


# --------------------------------------------------
# SAVE RAW WEATHER DATA
# --------------------------------------------------

def save_raw_data(city, weather_data, run_path, ingestion_time):

    city_name = re.sub(
        r"[^a-zA-Z0-9_-]",
        "_",
        city["city"].lower()
    )

    file_path = run_path / f"{city_name}.json"

    raw_record = {
        "ingestion_metadata": {
            "source": "Open-Meteo API",
            "ingested_at": ingestion_time,
            "state": city["state"],
            "city": city["city"],
            "latitude": city["latitude"],
            "longitude": city["longitude"]
        },
        "api_response": weather_data
    }

    with open(file_path, "w", encoding="utf-8") as file:

        json.dump(
            raw_record,
            file,
            indent=4,
            ensure_ascii=False
        )

    return file_path


# --------------------------------------------------
# MAIN INGESTION FUNCTION
# --------------------------------------------------

def run_ingestion():

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )

    ingestion_time = datetime.now(timezone.utc).isoformat()

    run_path = BRONZE_PATH / run_id

    run_path.mkdir(parents=True, exist_ok=True)

    successful = 0
    failed = 0

    failed_locations = []

    logger.info("=" * 60)
    logger.info("WEATHER DATA INGESTION STARTED")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Total locations: {len(CITIES)}")
    logger.info("=" * 60)

    session = create_session()

    try:

        for index, city in enumerate(CITIES, start=1):

            location = f"{city['city']}, {city['state']}"

            try:

                logger.info(
                    f"[{index}/{len(CITIES)}] Fetching weather for {location}"
                )

                weather_data = fetch_weather(session, city)

                file_path = save_raw_data(
                    city,
                    weather_data,
                    run_path,
                    ingestion_time
                )

                record_count = len(weather_data["hourly"]["time"])

                successful += 1

                logger.info(
                    f"SUCCESS | {location} | "
                    f"Records: {record_count} | "
                    f"File: {file_path.name}"
                )

            except (
                requests.RequestException,
                ValueError,
                OSError
            ) as error:

                failed += 1

                failed_locations.append({
                    "state": city["state"],
                    "city": city["city"],
                    "error": str(error)
                })

                logger.error(
                    f"FAILED | {location} | {error}"
                )

            time.sleep(0.2)

    finally:

        session.close()

    # Save failed location details

    if failed_locations:

        failure_file = run_path / "failed_locations.json"

        with open(failure_file, "w", encoding="utf-8") as file:

            json.dump(
                failed_locations,
                file,
                indent=4,
                ensure_ascii=False
            )

    # Final summary

    logger.info("=" * 60)
    logger.info("WEATHER INGESTION COMPLETED")
    logger.info(f"Total locations: {len(CITIES)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Bronze output: {run_path}")
    logger.info("=" * 60)

    return {
        "run_id": run_id,
        "total_locations": len(CITIES),
        "successful": successful,
        "failed": failed,
        "output_path": str(run_path)
    }


# --------------------------------------------------
# EXECUTION ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    run_ingestion()
