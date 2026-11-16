# Environmental Data Intelligence Pipeline

A batch data engineering project that collects hourly weather data for 36 Indian cities using the Open-Meteo API, processes it through Bronze, Silver, and Gold layers using PySpark, and generates analytics-ready datasets for an interactive dashboard.

The pipeline uses **Prefect** to orchestrate the ingestion and processing stages. A standalone **HTML dashboard**, built with HTML, CSS, JavaScript, and Chart.js, presents weather patterns, city comparisons, and daily trends.

## Dashboard Preview

[▶ Watch the Environmental Data Intelligence Dashboard Demo](docs/Environmental_Data_Intelligence_Dashboard.mp4)

## Project Overview

The project demonstrates an end-to-end batch data engineering workflow, including:

- API-based weather data ingestion
- Raw data storage using a Bronze layer
- Data cleaning, validation, and deduplication using PySpark
- Incremental processing in the Silver layer
- Analytical transformations in the Gold layer
- Workflow orchestration using Prefect
- CSV generation for dashboard consumption
- Interactive weather analytics and visualizations

The pipeline currently runs locally using PySpark in local mode.

## Architecture

```text
                 Open-Meteo API
                        |
                        v
              +-------------------+
              |  Bronze Layer     |
              |  Raw JSON Data    |
              +-------------------+
                        |
                        v
              +-------------------+
              |  Silver Layer     |
              |  PySpark Cleaning |
              |  Validation       |
              |  Deduplication    |
              +-------------------+
                        |
                        v
              +-------------------+
              |  Gold Layer       |
              |  Business Logic   |
              |  Aggregations     |
              |  Weather Insights |
              +-------------------+
                        |
                        v
              +-------------------+
              |    CSV Export     |
              +-------------------+
                        |
                        v
              +-------------------+
              |   HTML Dashboard  |
              |   Chart.js        |
              +-------------------+

          Prefect orchestrates the pipeline
```

## Data Source

The project uses the [Open-Meteo Weather API](https://open-meteo.com/).

Weather data is collected for **36 representative Indian cities**, covering all 28 states and 8 union territories.

### Weather Parameters

| Parameter | Description |
|---|---|
| Temperature | Hourly temperature in °C |
| Relative Humidity | Relative humidity percentage |
| Precipitation | Hourly precipitation in mm |
| Wind Speed | Wind speed in km/h |
| Weather Code | Weather condition classification |
| Timestamp | Date and time of the weather record |

The ingestion configuration retrieves the previous day's and current day's hourly data using the API's timezone-aware response.

## Data Engineering Workflow

### 1. Bronze Layer – Raw Data Ingestion

The Bronze layer collects weather data from the Open-Meteo API and stores the original responses as JSON files.

**Key operations:**
- Fetch hourly weather data for all configured cities.
- Store raw API responses separately for each city and ingestion run.
- Capture metadata such as city, state, coordinates, source, and ingestion timestamp.
- Implement retry handling with backoff for temporary API failures.
- Record failed city requests in `failed_locations.json` without stopping the entire ingestion process.

**Storage format:** JSON

### 2. Silver Layer – Data Cleaning and Transformation

The Silver layer uses PySpark to transform raw JSON data into structured, analytics-ready records.

**Key operations:**
- Flatten nested hourly weather arrays into individual records.
- Apply appropriate data types to numerical and timestamp columns.
- Extract date and hour information from timestamps.
- Remove records with missing key fields.
- Merge newly ingested data with the existing Silver dataset.
- Deduplicate records using `state`, `city`, and `timestamp`, retaining the latest ingestion.
- Partition the output by state for organized storage.

An incremental processing guard prevents already-processed Bronze batches from being processed again.

**Storage format:** Parquet

### 3. Gold Layer – Analytics and Business Transformations

The Gold layer applies business logic and aggregations to produce three analytical datasets.

#### A. daily_weather

Provides daily weather summaries for each city.

Metrics include:
- Average, minimum, and maximum temperature
- Average humidity
- Average wind speed
- Total precipitation
- Hourly record count
- Rainy-day indicator

#### B. city_weather_insights

Provides city-level weather insights using daily metrics and derived classifications.

Includes:
- Temperature, humidity, and wind categories
- Weather condition classification
- City-level temperature rankings
- Daily weather indicators

#### C. weather_trends

Tracks day-over-day changes in weather conditions.

Includes:
- Temperature change
- Rainfall change
- Wind speed change
- Trend classifications such as Warmer, Cooler, and Stable

The first available day is treated separately because there is no previous-day record for comparison.

**Storage format:** Parquet

### Weather Classification Rules

The Gold layer applies the following business rules:

| Category | Classification |
|---|---|
| Cool | Temperature below 15°C |
| Moderate | Temperature up to 25°C |
| Warm | Temperature up to 35°C |
| Hot | Temperature above 35°C |
| Low Humidity | Below 30% |
| Moderate Humidity | Up to 60% |
| High Humidity | Above 60% |
| Calm Wind | Below 10 km/h |
| Moderate Wind | Up to 25 km/h |
| Strong Wind | Above 25 km/h |

Weather categories:

- **Hot and Rainy:** Rainfall recorded and average temperature at least 30°C.
- **Rainy:** Rainfall recorded without meeting the Hot and Rainy condition.
- **Hot and Dry:** Average temperature at least 30°C with no rainfall.
- **Normal:** Other conditions.

Temperature trend classification:

- **Warmer:** Temperature increases by more than 0.5°C.
- **Cooler:** Temperature decreases by more than 0.5°C.
- **Stable:** Temperature change is within 0.5°C.

## Workflow Orchestration

The pipeline is orchestrated using **Prefect**.

The orchestration flow executes the stages in sequence:

1. Bronze ingestion
2. Silver processing
3. Gold transformations

Additional workflow features include:
- Task dependency management
- Automatic retries for failed tasks
- Retry delay configuration
- Execution logging
- Reuse of completed Gold outputs where applicable

The current Prefect configuration uses two retries with a 30-second delay.

## Interactive Dashboard

The project includes a standalone HTML dashboard built using:

- HTML
- CSS
- JavaScript
- Chart.js

### Dashboard Features

- **Overview:** Displays key weather metrics and overall weather conditions.
- **Compare:** Compares weather metrics across different cities.
- **City Explorer:** Explores weather information for individual cities.
- **Daily Change:** Displays changes in weather conditions over time.
- **Date Filters:** Filters dashboard data by date.
- **CSV Integration:** Automatically loads the exported city insights CSV when the dashboard is served locally.

The dashboard fetches:

`data/powerbi/city_weather_insights.csv`

If automatic loading is unavailable, the dashboard also retains a manual CSV upload option.

## Technology Stack

| Area | Technologies |
|---|---|
| Programming Language | Python |
| Data Processing | PySpark |
| Data Storage | JSON, Parquet |
| Data Source | Open-Meteo API |
| Workflow Orchestration | Prefect |
| Data Export | CSV |
| Dashboard | HTML, CSS, JavaScript |
| Visualization | Chart.js |
| Development Environment | Python Virtual Environment |

## Project Structure

```text
ENVIRONMENTAL_DATA_INTELLIGENCE_PIPELINE/
│
├── config/
│   └── cities.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── powerbi/
│
├── dashboard/
│   └── index.html
│
├── docs/
│   ├── dashboard.png
│   └── Environmental_Data_Intelligence_Dashboard.mp4
│
├── logs/
│
├── sample_data/
│
├── src/
│   ├── ingestion/
│   │   └── weather_api.py
│   │
│   ├── processing/
│   │   └── weather_cleaning.py
│   │
│   ├── transformations/
│   │   └── weather_gold.py
│   │
│   ├── orchestration/
│   │   └── environmental_pipeline.py
│   │
│   └── exports/
│       └── export_gold_for_powerbi.py
│
├── README.md
└── requirements.txt
```

## Installation and Setup

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd ENVIRONMENTAL_DATA_INTELLIGENCE_PIPELINE
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
```

Activate it on Windows:

```powershell
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Verify Java and PySpark

Check Java:

```bash
java -version
```

Check PySpark:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

The project uses PySpark 3.5.6 and requires a compatible Java installation.

## Running the Pipeline

Run the complete pipeline from the project root:

```bash
python -m src.orchestration.environmental_pipeline
```

This executes Bronze ingestion, Silver processing, and Gold transformations through Prefect.

### Run Individual Stages

**Bronze – API Ingestion**

```bash
python -m src.ingestion.weather_api
```

**Silver – Data Cleaning**

```bash
python -m src.processing.weather_cleaning
```

**Gold – Analytics Transformations**

```bash
python -m src.transformations.weather_gold
```

## Exporting Data for the Dashboard

After the pipeline completes, run:

```bash
python -m src.exports.export_gold_for_powerbi
```

This exports the Gold datasets into CSV format under:

```text
data/powerbi/
```

The dashboard uses the exported `city_weather_insights.csv` file.

## Running the Dashboard

Start the local HTTP server from the project root:

```bash
python -m http.server 8000
```

Open the dashboard in your browser:

[http://localhost:8000/dashboard/](http://localhost:8000/dashboard/)

The dashboard automatically fetches the latest exported CSV when available.

**Important:** Keep the HTTP server running while using the dashboard. Opening the HTML file directly through `file://` may prevent automatic CSV loading.

To stop the server, press `Ctrl + C` in the terminal.

## Incremental Processing and Reliability

The project incorporates several data engineering practices:

- **Incremental ingestion:** New API data is collected during each pipeline execution.
- **Idempotent Silver processing:** Previously processed Bronze batches are skipped.
- **Deduplication:** Records are identified using city, state, and timestamp.
- **Retry handling:** Temporary API and workflow failures are handled through retry mechanisms.
- **Failure tracking:** Failed city requests are logged separately.
- **Partitioned storage:** Silver data is organized by state.
- **Run-specific outputs:** Processing stages maintain separate execution outputs.
- **Output validation:** Completed Gold datasets can be reused, while incomplete outputs are handled separately.

## Current Implementation and Scope

This project is currently implemented as a local batch data pipeline.

- PySpark runs in local mode rather than on a distributed cluster.
- Weather data is retrieved through the Open-Meteo API.
- The pipeline processes weather data only.
- Prefect handles workflow orchestration.
- Parquet is used for structured analytical storage.
- CSV files connect the Gold layer to the HTML dashboard.

The architecture demonstrates data engineering concepts that can be extended to cloud-based environments using services such as Amazon S3, AWS Glue, Amazon Athena, and managed workflow orchestration tools.

## Future Enhancements

- Deploy the pipeline on AWS using S3 and AWS Glue.
- Automate scheduled pipeline execution.
- Introduce data quality checks and monitoring.
- Add historical weather analysis over longer periods.
- Optimize processing and storage for larger datasets.
- Integrate a cloud-based analytics and visualization platform.

## Data Source and Attribution

Weather data is provided by [Open-Meteo](https://open-meteo.com/).

API documentation: [Open-Meteo API Documentation](https://open-meteo.com/en/docs).

---

**Project:** Environmental Data Intelligence Pipeline  
**Focus:** Data Engineering | PySpark | ETL | Prefect | Data Analytics
