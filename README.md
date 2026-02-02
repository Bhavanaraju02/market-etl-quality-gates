# Market Data ETL with Data Quality Gates (Python + SQL + Docker)

A production-style **ETL pipeline** that ingests daily stock market OHLCV data into **PostgreSQL** using **raw → staging → curated** layers, enforces **data quality gates** (null/dup/range/schema), runs **pytest regression tests**, and generates **run artifacts** (JSON validation reports) for monitoring and incident triage.

---

## What this project does (simple explanation)

In real companies, data comes from external sources (vendors/APIs) and can be incomplete, inconsistent, or change over time.  
This project simulates a real data engineering workflow:

1. **Extract** daily OHLCV (Open/High/Low/Close/Volume) for selected stock symbols from **Stooq** (free, no API key).
2. **Transform** the data (type conversions, missing values removal, basic sanity filters).
3. **Load** into Postgres with 3 layers:
   - **Raw**: keeps an audit snapshot for every pipeline run
   - **Staging**: cleaned, deduplicated “latest” state (idempotent upserts)
   - **Curated**: final table with derived metrics (e.g., daily return)

Then it runs **data quality checks** and writes a **JSON validation report** for every run.

---

## Tech Stack

- **Python** (ETL + validations)
- **PostgreSQL** (warehouse)
- **Docker / Docker Compose** (run Postgres locally)
- **SQLAlchemy + psycopg2** (DB connectivity)
- **pytest** (automated tests)
- **requests + pandas** (data download + processing)

---

## Repository Structure

market-etl-quality-gates/
├── etl/
│ ├── db.py # DB connection helper (.env)
│ ├── schema.py # Creates tables (DDL)
│ ├── extract.py # Extract from Stooq CSV
│ ├── transform.py # Cleaning + typing
│ ├── load.py # Raw load + staging upsert + curated build
│ └── run_pipeline.py # End-to-end pipeline runner
├── checks/
│ └── quality_checks.py # SQL checks + schema drift-lite + vendor validation
├── tests/
│ └── test_transformations.py # pytest regression tests
├── reports/ # Output validation reports per run
├── docker-compose.yml # Postgres container
├── requirements.txt # Python dependencies
├── .env # DB credentials (DO NOT commit)
└── README.md


---

## Data Model (Raw → Staging → Curated)

### 1) `raw_prices` (audit snapshot per run)
- Stores each run’s ingested data with a `run_id`.
- Keeps history for compliance / debugging vendor changes.

### 2) `stg_prices` (cleaned “latest” state)
- Deduplicated by `(symbol, date)` with **idempotent upserts**.
- Safe to rerun pipelines and backfills.

### 3) `fct_prices_daily` (curated facts)
- Final analytics-ready table built from staging.
- Adds derived features:
  - `daily_return`
  - `typical_price`

> Why raw count can be higher than staging/curated:  
> raw stores *every run snapshot*, while staging/curated store a *unique latest view*.

---

## Setup (Local)

### Prerequisites
- Python **3.10+**
- Docker Desktop
- Git

---

## Quickstart

### 1) Start PostgreSQL with Docker
``
docker compose up -d
2) Create and activate a Python environment
python3 -m venv .venv
source .venv/bin/activate
3) Install dependencies
pip install -r requirements.txt
4) Add DB credentials
Create a .env file in the repo root:
DB_HOST=localhost
DB_PORT=5432
DB_NAME=market
DB_USER=etl
DB_PASSWORD=etl
5) Run tests
python -m pytest -q
6) Run the pipeline (one command)
python -m etl.run_pipeline --symbols AAPL.US MSFT.US NVDA.US --start 2023-01-01

## You should see:
PIPELINE SUCCESS
A JSON report created in reports/:
reports/validation_report_<run_id>.json
Data Quality Gates (Correctness Checks)
This project enforces correctness using SQL-based validations:
Schema drift-lite
Ensures curated table still has expected columns
Null checks
Keys (symbol, date) must never be null
Duplicate checks
(symbol, date) must be unique in curated data
Range checks
Prices must be > 0
high >= low
volume >= 0
Vendor delivery validation
Compares current run’s row count vs previous successful run
Flags large deltas (potential vendor anomalies)
If any check fails, the pipeline:
writes a report with failure reasons
exits with non-zero code (useful for automation/CI)



## Inspect Data in Postgres (Optional)
Open psql inside the container:
docker exec -it market_etl_pg psql -U etl -d market
Run:
SELECT COUNT(*) FROM raw_prices;
SELECT COUNT(*) FROM stg_prices;
SELECT COUNT(*) FROM fct_prices_daily;
SELECT * FROM fct_prices_daily LIMIT 5;
Exit:
\q
Example Output (Validation Report)
Each run creates:
{
  "run_id": "20260202T182512Z",
  "symbols": ["AAPL.US", "MSFT.US", "NVDA.US"],
  "start": "2023-01-01",
  "end": null,
  "status": "SUCCESS",
    "issues": []
}
If issues exist:
status: "FAILED"
issues: ["..."]
