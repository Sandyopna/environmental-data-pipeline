# Environmental Data Intelligence Pipeline

A batch data pipeline that ingests hourly weather data from the Open-Meteo API for 36 Indian cities, processes it through Bronze, Silver and Gold layers with PySpark, and produces analytics-ready tables for dashboards. The stages are orchestrated with Prefect.

![Dashboard screenshot](docs/dashboard.png)
<!-- Add a screenshot of your dashboard at docs/dashboard.png -->

## Architecture

```
Open-Meteo API
      |
      v
  Bronze   raw JSON, one file per city per run
      |
      v
  Silver   cleaned, typed Parquet, merged and deduplicated across runs
      |
      v
   Gold    daily_weather | city_weather_insights | weather_trends
      |
      v
  CSV export  ->  dashboards (Power BI / HTML)
```

Prefect runs Bronze, Silver and Gold in order, with retries on each task. The CSV export is a separate script that can be run after the flow.

## Tech stack

Python, PySpark (local mode), Prefect, Parquet, Open-Meteo API, Power BI-ready CSV export.

## Layers

### Bronze: ingestion (`src/ingestion/weather_api.py`)
- Calls the Open-Meteo forecast endpoint for each city in `config/cities.py`, requesting hourly temperature, relative humidity, precipitation, wind speed and weather code.
- Fetches yesterday plus today (`past_days=1`, `forecast_days=1`).
- Uses an HTTP session with retries and backoff for rate limits and server errors.
- Saves each response as raw JSON with ingestion metadata (source, timestamp, city, state, coordinates).
- Failed cities are written to `failed_locations.json` so one failure does not stop the run.

### Silver: cleaning (`src/processing/weather_cleaning.py`)
- Reads the latest Bronze run and flattens the hourly arrays into one row per city per hour.
- Casts types, derives `date` and `hour`, and drops rows with missing keys.
- Merges the new data with the previous Silver snapshot and keeps the most recently ingested record for each `state + city + timestamp`, so overlapping runs do not create duplicates.
- Writes Parquet partitioned by state. Reruns of an already processed Bronze batch are skipped.

### Gold: analytics (`src/transformations/weather_gold.py`)
Three datasets are built from the latest Silver snapshot.

| Dataset | Description |
|---|---|
| `daily_weather` | Daily aggregates per city: average, min and max temperature, average humidity and wind, total precipitation, hourly record count, rainy-day flag |
| `city_weather_insights` | Daily data plus category labels and daily rankings (temperature, rainfall, wind) |
| `weather_trends` | Day-over-day changes in temperature, rainfall and wind, with trend labels |

**Classification rules**

| Field | Rule |
|---|---|
| `temperature_category` | Cool below 15 °C, Moderate up to 25, Warm up to 35, Hot above |
| `humidity_category` | Low below 30%, Moderate up to 60%, High above |
| `wind_category` | Calm below 10 km/h, Moderate up to 25, Strong above |
| `weather_category` | Hot and Rainy (rain and avg temp of 30 °C or more), Rainy, Hot and Dry (30 °C or more, no rain), otherwise Normal |
| `temperature_trend` | Warmer if up more than 0.5 °C, Cooler if down more than 0.5 °C, otherwise Stable |

The first day for each city has no previous value, so its trend fields are labelled "Insufficient History".

### Export (`src/exports/export_gold_for_powerbi.py`)
Reads the latest completed run of each Gold dataset and writes a single CSV per dataset to `data/powerbi/`.

## Reliability

- Each layer writes into a run-specific folder and the next layer only reads folders that contain a `_SUCCESS` marker, so incomplete output is never consumed.
- Silver processing is idempotent: running it again on a processed batch is a no-op.
- Gold reuses datasets that already exist for a Silver run and fails loudly on incomplete ones.
- Prefect retries each stage (2 retries, 30 second delay) and propagates failures.
- Logs are written to `logs/`.

## Project structure

```
.
├── config/
│   └── cities.py                  # city, state, latitude, longitude
├── src/
│   ├── ingestion/weather_api.py
│   ├── processing/weather_cleaning.py
│   ├── transformations/weather_gold.py
│   ├── orchestration/environmental_pipeline.py
│   └── exports/export_gold_for_powerbi.py
├── data/                          # generated (not committed)
│   ├── bronze/ silver/ gold/ powerbi/
├── logs/                          # generated (not committed)
├── sample_data/                   # example Gold CSV output
└── requirements.txt
```

## Getting started

**Prerequisites:** Python 3.10 or later and a Java runtime (required by PySpark).

```bash
git clone https://github.com/Sandyopna/environmental-data-pipeline.git
cd environmental-data-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Run the full pipeline** (Bronze, Silver, Gold):

```bash
python -m src.orchestration.environmental_pipeline
```

**Run a single stage:**

```bash
python -m src.ingestion.weather_api
python -m src.processing.weather_cleaning
python -m src.transformations.weather_gold
```

**Export CSVs for dashboards:**

```bash
python -m src.exports.export_gold_for_powerbi
```

The CSVs are written to `data/powerbi/`.

## Sample output

`sample_data/city_weather_insights.csv` is an example of the Gold output. It can be loaded into any BI tool or a CSV-driven dashboard.

## Limitations

- This is a **batch** pipeline, not a streaming or real-time one. Data is as fresh as the last run.
- Open-Meteo's forecast endpoint returns weather model data, and today's values are partly forecast, so this is not a record of station measurements.
- Spark runs in local mode on a small dataset. The code is structured in layers, but it has not been tested at larger scale.
- Scheduling is not enabled by default. The flow can be served on a schedule with Prefect, or run with an OS scheduler.
- The CSV export is a separate step and is not part of the Prefect flow.

## Possible improvements

- Schedule the flow with a Prefect deployment and add the export as a final task.
- Add data quality checks, such as value ranges and row counts per city, between layers.
- Add unit tests for the Silver merge and Gold classification logic.
- Move storage from local folders to object storage.

## Data source

Weather data from the [Open-Meteo API](https://open-meteo.com/).
