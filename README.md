# 🌊 Marisight — Maritime Risk & Operations Analytics Platform

> **ITI Data Engineering Track · Graduation Project · R2 2026**

Marisight is an end-to-end modern data platform that ingests, transforms, and serves maritime intelligence across three data domains: **global port metadata**, **real-time vessel tracking**, and **live seismic events**. The platform delivers operational dashboards for port safety analysis, vessel monitoring, and an AI-powered smart port recommendation engine.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Repository Structure](#repository-structure)
- [Tech Stack](#tech-stack)
- [Data Pipeline](#data-pipeline)
  - [1. Ingestion Layer](#1-ingestion-layer)
  - [2. Staging Layer (Amazon S3)](#2-staging-layer-amazon-s3)
  - [3. Snowflake Medallion Architecture](#3-snowflake-medallion-architecture)
  - [4. Orchestration (Apache Airflow)](#4-orchestration-apache-airflow)
  - [5. Serving Layer](#5-serving-layer)
- [dbt Transformation Models](#dbt-transformation-models)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [AI Port Recommendation Engine](#ai-port-recommendation-engine)
- [Streaming Analytics](#streaming-analytics)
- [Data Quality](#data-quality)
- [Team](#team)
- [Setup & Quickstart](#setup--quickstart)
- [Known Limitations](#known-limitations)

---

## Project Overview

Maritime operations depend on fast, reliable, and integrated data. Port operators, vessel dispatchers, and risk analysts currently work across siloed datasets — static port directories, fragmented voyage reports, and manually reviewed seismic bulletins — with no unified view.

**Marisight** solves this by building a production-grade ELT pipeline that:

- Automates data collection from heterogeneous sources on different cadences (monthly batch, daily scrape, real-time stream)
- Unifies all data inside a Snowflake data warehouse using the Bronze → Silver → Gold medallion pattern
- Applies dbt-managed transformations for cleaning, enrichment, and business-logic scoring
- Powers two serving layers: **Power BI** for batch analytical dashboards and **Grafana** for real-time seismic monitoring
- Exposes an **AI-powered port recommendation engine** that scores and ranks ports for each active vessel using multi-dimensional weighted scoring

---

## Architecture

[<img width="1537" height="1023" alt="image" src="https://github.com/user-attachments/assets/260d3ec2-94d3-41b7-8363-03a63d2d0101" />
](https://github.com/Marisight/Marisight-Maritime-Risk-Operations-Analytics-Platform/blob/main/Visualization/last_arch.jpeg?raw=true)

---

## Data Sources

| Source | Domain | Format | Cadence | Ingestion Method |
|---|---|---|---|---|
| **NGA World Port Index** | Port metadata | CSV via API | Monthly | AWS Lambda → S3 → Snowflake Bronze |
| **VesselFinder** | Vessel tracking & voyages | HTML scrape → CSV | Daily | AWS Lambda → S3 → Snowflake Bronze |
| **EMSC Seismic API** | Global earthquake events | REST/JSON → PostgreSQL | Real-time (CDC) | Debezium → Kafka → Snowflake + ClickHouse |

**Port data** covers 3,804 ports with 109 attributes per port: geography, depth constraints, cargo facilities, supply availability, communications, safety infrastructure, and more.

**Vessel data** captures daily snapshots of active vessels: voyage status, departure/arrival dates, destination coordinates, vessel type, and physical dimensions.

**Seismic data** streams live events from the European Mediterranean Seismological Centre (EMSC), capturing magnitude, depth, location, and event classification for seismic risk proximity scoring.

---

## Repository Structure

```
Marisight-Maritime-Risk-Operations-Analytics-Platform/
│
├── Streaming_ingestion/          # Real-time pipeline components
│   ├── Dockerfile
│   ├── docker-compose.yaml       # Kafka + Debezium + ClickHouse stack
│   ├── ingest.py                 # PostgreSQL seismic event poller
│   └── req.txt
│
├── Visualization/                # Power BI reports (.pbix)
│
├── airflow/
│   └── dags/                     # Airflow DAG definitions
│       ├── ports_data.py         # Monthly ports batch DAG
│       ├── seismic_data.py       # Seismic monitoring DAG
│       └── vessels_data.py       # Daily vessel scrape DAG
│
├── batch_extract/
│   ├── ports/
│   │   ├── api_2026-05.csv       # Raw port data snapshot
│   │   └── lambda_function.py    # AWS Lambda: ports ingestion
│   └── vessels/
│       ├── base_scraper.py
│       ├── data_quality.py
│       ├── lambda_function.py    # AWS Lambda: vessel scraping
│       └── vessel_scraper.py
│
├── marisight_dbt/                # dbt Core project
│   ├── models/
│   │   ├── silver/               # Bronze → Silver cleaning models
│   │   |   ├── schema.yml 
│   │   │   ├── silver_ports.sql
│   │   │   ├── silver_vessels.sql
│   │   │   └── silver_seismic_events.sql
│   │   └── gold/                 # Silver → Gold business logic models
│   │       ├── daily_aggregated_seismic_events.sql
│   │       ├── dim_datesql
│   │       ├── dim_port.sql
│   │       ├── dim_seismic_eventsql
│   │       ├── fact_daily_seismic.sql
│   │       ├── fact_seismic_port_proximity.sql
│   │       ├── fact_vessel_voyagesql
│   │       ├── gold_ports.sql
│   │       ├── gold_seismic_eventssql
│   │       ├── gold_seismic_port_proximity.sql
│   │       ├── gold_vessels.sql
│   │       ├── gold_vessels_with_port_details.sql
│   │       └── schema-yaml
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── generate_schema_name.sql   # Custom schema routing macro
│   ├── sources/
│   │   └── sources.yml
│   ├── tests/
│   ├── dbt_project.yml
│   └── README.md
│
├── snowflake/
│   ├── set_up_ports.sql          # Snowflake DDL: Bronze ports table
│   └── set_up_vessels.sql        # Snowflake DDL: Bronze vessels table
│
├── streaming-analytics/          # Spark Streaming + ClickHouse
│   ├── docker-compose.yaml
│   ├── grafana_dashboard.json    # Grafana dashboard definition
│   └── cli.sql                   # ClickHouse schema setup
│
├── .gitignore
└── README.md
```

---

## Tech Stack

| Category | Technology |
|---|---|
| **Cloud Platform** | AWS (Lambda, S3, EventBridge |
| **Data Warehouse** | Snowflake |
| **Transformation** | dbt Core (local, submitting SQL to Snowflake) |
| **Streaming Ingestion** | Apache Kafka, Debezium (CDC), Kafka Connect Snowflake Connector |
| **Real-time Store** | ClickHouse |
| **Orchestration** | Apache Airflow (local) |
| **Batch Ingestion** | AWS Lambda (Python), AWS EventBridge |
| **Staging** | Amazon S3 (external stage) |
| **BI / Dashboards** | Power BI Desktop |
| **Real-time Monitoring** | Grafana |
| **AI / Scoring** | Python (custom multi-factor scoring engine) |
| **Languages** | Python, SQL |
| **Version Control** | GitHub |
| **Containerisation** | Docker, Docker Compose |

---

## Data Pipeline

### 1. Ingestion Layer

#### Batch Ingestion (AWS Lambda)

Two Lambda functions handle periodic batch collection:

**Marisight_Ports_API** — runs monthly via AWS EventBridge. Downloads the NGA World Port Index CSV (~3,804 rows, 109 columns), uploads directly to the designated S3 prefix, and triggers a Snowflake COPY INTO to land data in `PROJECT_DB.DBO.PORTS`.

**Marisight_Vessel_Scraper** — runs daily via EventBridge. Executes a rate-limited web scraper against VesselFinder's vessel listings, collects active vessel records as CSV, uploads to S3 under a date-partitioned prefix, and triggers COPY INTO to `PROJECT_DB.DBO.VESSEL`. All columns are stored as VARCHAR at Bronze to preserve raw fidelity.

#### Streaming Ingestion (Kafka / Debezium)

Seismic events flow through a real-time CDC pipeline:

1. An ingestion script polls the EMSC REST API and writes events into **PostgreSQL** (`marisight.public.seismic_events`)
2. **Debezium** monitors the PostgreSQL WAL and publishes row-level change events to a **Kafka** topic
3. Events fork to two consumers:
   - **Kafka Connect Snowflake Connector** → `PROJECT_DB.DBO."marisight.public.seismic_events"` (Bronze). Used by dbt for analytical transformation.
   - **Kafka** → **ClickHouse** (`seismic_events` table). Used by Grafana for sub-minute real-time monitoring.

> **Note on Kafka connector schema inference:** The Snowflake connector infers `LAT`, `LON`, `MAG`, and `DEPTH` as `NUMBER(38,0)`, losing decimal precision at ingestion. This is accepted as an unrecoverable upstream constraint and documented throughout the dbt Silver model.

---

### 2. Staging Layer (Amazon S3)

Amazon S3 acts as the durable intermediate staging layer for all batch data:

- Organized under prefixes by domain and date: `s3://marisight-staging-layer/ports/`, `s3://marisight-staging-layer/vessels/`
- Serves as both a trigger point for Snowflake COPY INTO and a long-term backup/audit trail
- S3 lifecycle policies applied to manage storage costs within a student-tier budget

---

### 3. Snowflake Medallion Architecture

All analytical data converges in Snowflake, organized in three schemas within `PROJECT_DB`:

#### Bronze (`PROJECT_DB.DBO`)
Raw, unmodified data as landed from ingestion. All VARCHAR for scraped data; Kafka-native types for seismic. No transformations applied. Serves as the single source of truth for all downstream models.

| Table | Source | Cadence |
|---|---|---|
| `DBO.VESSEL` | VesselFinder scrape | Daily |
| `DBO.PORTS` | NGA World Port Index | Monthly |
| `DBO."marisight.public.seismic_events"` | Kafka/Debezium CDC | Real-time |

#### Silver (`PROJECT_DB.SILVER`)
Cleaned, typed, validated data. One row per business entity (vessel per report date, port, seismic event). Managed entirely by dbt incremental models.

| Table | Grain | Key Transformations |
|---|---|---|
| `SILVER.SILVER_VESSELS` | Vessel × Report Date | Type casting, date parsing, coordinate validation, temporal anomaly flags, deduplication |
| `SILVER.SILVER_PORTS` | Port (by index number) | Categorical normalisation, 0-sentinel → NULL for depths, coordinate validation |
| `SILVER.SILVER_SEISMIC_EVENTS` | Seismic event (UNID) | CDC field stripping, human-readable labels, FLOAT casting, deduplication |

#### Gold (`PROJECT_DB.GOLD`)
Business-logic enriched, analytics-ready tables. Consumed directly by Power BI and the AI recommendation engine.

| Table | Purpose |
|---|---|
| `GOLD_VESSELS` | Enriched vessel records with age categories, size categories, voyage status flags |
| `GOLD_PORTS` | Port records with computed supply scores, communication scores, depth classification |
| `GOLD_VESSELS_WITH_PORT_DETAILS` | Vessel-to-destination port join with compatibility flags |
| `GOLD_PORT_SUPPLY_DETAILS` | Supply and communication binary flags per port with aggregate scores |
| `GOLD_PORT_INFRASTRUCTURE_FACILITIES` | Facility availability matrix per port with primary type classification |
| `GOLD_SEISMIC_EVENTS` | Classified seismic events with magnitude category |
| `GOLD_SEISMIC_PORT_PROXIMITY` | Distance and risk score for every (event, port) pair within proximity threshold |
| `DAILY_AGGREGATED_SEISMIC_EVENTS` | Regional daily rollup with rolling 7-day averages and risk level classification |
| `GOLD_PORT_RECOMMENDATIONS_V2` | AI engine output — top-N ranked ports per vessel with full scoring breakdown |

---

### 4. Orchestration (Apache Airflow)

Apache Airflow runs locally and owns the end-to-end pipeline schedule:

```
DAG: vessels_data          → Daily    → Lambda trigger → Snowflake COPY → dbt Silver → dbt Gold → AI Engine
DAG: ports_data            → Monthly  → Lambda trigger → Snowflake COPY → dbt Silver → dbt Gold
DAG: seismic_data          → Sensor   → Kafka connector health check → dbt Silver seismic → Gold proximity scoring
```

Key design choices:
- All dbt models invoked via `BashOperator` calling `dbt run --select <model>` with explicit dependency sequencing
- The AI recommendation engine Python script is wired as the **final task** in the vessels DAG, executing after all Gold models complete
- Retries configured with exponential back-off; failure alerts via Airflow email notification
- All DAGs and dbt models designed for full idempotency — safe to rerun or backfill without duplicating data

---

### 5. Serving Layer

#### Power BI (Batch Analytics)
Connects directly to Snowflake Gold layer via the Snowflake Power BI connector. Dashboards cover:
- Vessel fleet overview (voyage status, type distribution, destination mapping)
- Port capability comparison (depth, supply scores, facilities by region)
- Seismic risk heatmap overlaying port locations with recent event proximity scores
- Operational KPIs for route planning and port selection

#### Grafana (Real-time Monitoring)
Connects to ClickHouse to power sub-minute dashboards:
- Live seismic event feed with magnitude and depth visualisation
- Event frequency time-series with rolling average overlays
- Geographic alert panels for high-magnitude events near active shipping routes
- Configured alert rules with notification thresholds

---

## dbt Transformation Models

The dbt project (`marisight_dbt`) uses dbt Core running locally, targeting Snowflake as the execution engine.

### Silver Layer

All Silver models use `materialized: incremental` with MERGE semantics.

#### `silver_vessels`
- **Grain:** One row per vessel per `REPORT_DATE`
- **Unique key:** `(NAME, REPORT_DATE)`
- **Incremental watermark:** `_INGESTED_AT` (monotonically increasing pipeline timestamp, preferred over VARCHAR `REPORT_DATE`)
- **Key logic:**
  - `TRY_TO_*` functions for safe type casting of all-VARCHAR Bronze columns
  - Regex-based date extraction from ATD/ATA/ETA strings (`'ATD: Mon DD, HH:MI UTC(...)'`)
  - Year-crossover resolution: ETA advances to `base_year + 1` for future dates; ATD/ATA fall back to `base_year - 1` for past dates near January
  - `IS_VALID_COORDINATES`, `IS_TEMPORAL_ANOMALY`, `IS_MISSING_TIMES` quality flags
  - `ROW_NUMBER()` deduplication: latest `_INGESTED_AT` wins per key

#### `silver_ports`
- **Grain:** One row per port (`WORLD_PORT_INDEX_NUMBER`)
- **Materialization:** Table (monthly source; full refresh acceptable)
- **Key logic:**
  - `clean_categorical()` macro normalises `'Unknown'`, `'-'`, `'N/A'`, blank → `NULL` across all categorical columns
  - `NULLIF(..., 0)` converts 0-sentinel depth/dimension values to `NULL`
  - Coordinate validity flag
  - `OID_` tiebreak for deduplication

#### `silver_seismic_events`
- **Grain:** One row per seismic event (`UNID` / `EVENT_ID`)
- **Unique key:** `EVENT_ID`
- **Incremental watermark:** `LAST_UPDATED_AT` with 24-hour lookback window to capture EMSC event revisions
- **Key logic:**
  - `identifier: '"marisight.public.seismic_events"'` in `sources.yml` to handle the Kafka-assigned dot-notation table name without renaming the table (connector owns it)
  - Human-readable labels for `EVTYPE` and `MAGTYPE` codes
  - `__DELETED = 'false'` filter (defensive CDC guard)
  - `FLOAT` cast for `LAT`, `LON`, `MAG`, `DEPTH` from `NUMBER(38,0)` — decimal precision unrecoverable, documented

### Gold Layer

Gold models materialise as `table` (full refresh each run). Business logic applied:

- **Vessel enrichment:** `VESSEL_AGE`, `AGE_CATEGORY` (New/Modern/Mature/Aging), `SIZE_CATEGORY` (by deadweight), `VOYAGE_STATUS`, `IS_ANCHORED`/`IS_MOORED`/`IS_UNDERWAY` boolean flags, `DAYS_SINCE_DEPARTURE`
- **Port scoring:**
  - `SUPPLY_SCORE` (0–5): counts available supply types (provisions, water, fuel oil, diesel, repairs)
  - `COMMUNICATION_SCORE` (0–4): counts available communication channels
  - `PORT_DEPTH_CLASS` (Shallow/Medium/Deep): bucketed from `CHANNEL_DEPTH_M`
  - `SUPPLY_RATING` / `COMMUNICATION_RATING`: human-readable tiers derived from scores
- **Seismic proximity:** Haversine-based distance calculation for all port-event pairs within a configurable km threshold; `RISK_SCORE` combining magnitude and proximity
- **Port-to-vessel join:** Fuzzy name matching between vessel `DESTINATION_PORT_NAME` and `GOLD_PORTS.MAIN_PORT_NAME` with uppercase normalisation and whitespace trimming

---

## AI Port Recommendation Engine

**File:** `sample_output/port_recommendations_engine.py`  
**Output table:** `PROJECT_DB.GOLD.GOLD_PORT_RECOMMENDATIONS_V2`

The engine runs as the final Airflow task after the Gold dbt run completes. For each active vessel it:

1. Pulls vessel dimensions and current reported status from `GOLD_VESSELS`
2. Filters the port universe to physically compatible ports (depth, length, beam constraints)
3. Scores every eligible port across **seven weighted dimensions:**

| Dimension | Weight | Source |
|---|---|---|
| Distance Score | W_DISTANCE | Haversine from vessel coordinates to port |
| Infrastructure Score | W_INFRA | Facility type match to vessel type |
| Supply Score | W_SUPPLY | `GOLD_PORT_SUPPLY_DETAILS.SUPPLY_SCORE` |
| Communication Score | W_COMM | `GOLD_PORTS.COMMUNICATION_SCORE` |
| Safety Score | W_SAFETY | Port security + SAR + medical + VTS flags |
| Depth Score | W_DEPTH | Channel depth vs. vessel draft clearance |
| Seismic Safety Score | W_SEISMIC | Inverted `GOLD_SEISMIC_PORT_PROXIMITY.RISK_SCORE` |

4. Computes `FINAL_SCORE = Σ(weight × normalised_dimension_score)`
5. Ranks ports per vessel and writes top-N results to `GOLD_PORT_RECOMMENDATIONS_V2`

> **Known data constraint:** All active vessels in the current dataset have `IS_VALID_COORDINATES = FALSE` due to upstream scraper data quality, resulting in `DISTANCE_KM = NULL`. The `W_DISTANCE` weight is redistributed to zero and reallocated proportionally across remaining dimensions. This is flagged in the output and documented as an upstream unresolvable issue.

---

## Streaming Analytics

**Stack:** Docker Compose → Kafka → Spark Streaming → ClickHouse → Grafana

The `streaming-analytics/` directory contains the full real-time analytics stack:

- `docker-compose.yaml` — orchestrates Kafka, Zookeeper, Spark, ClickHouse, and Grafana containers
- `cli.sql` — ClickHouse DDL for the `seismic_events` table
- `grafana_dashboard.json` — pre-built dashboard importable into Grafana

ClickHouse consumes from the same Kafka topic with sub-minute latency. This path is optimised for Grafana's live monitoring use case without competing with the batch analytical path.

---

## Data Quality

Data quality is enforced at multiple layers:

**dbt Tests (`schema.yml`)**
- `not_null` on all primary keys and critical timestamps
- `unique` on grain keys (`WORLD_PORT_INDEX_NUMBER`, `EVENT_ID`)
- `accepted_values` on `HARBOR_SIZE` with `severity: warn` (non-blocking — allows novel values to pass through while surfacing in dbt logs)
- Source freshness checks on `DBO.VESSEL`: warn after 25 hours, error after 49 hours

**Silver Transformation Flags**
- `IS_VALID_COORDINATES` — coordinate range check on all spatial fields
- `IS_TEMPORAL_ANOMALY` — flags vessels where ATA < ATD at date grain
- `IS_MISSING_TIMES` — flags vessels with no departure, arrival, or ETA data

**Incremental Idempotency**
- All incremental models use MERGE (not INSERT), preventing duplicate rows on reruns
- 24-hour lookback window on seismic watermark captures late-arriving EMSC event revisions
- `ROW_NUMBER()` deduplication within each batch as a second safety layer

**Known Accepted Issues**
- Seismic `LAT`/`LON`/`MAG`/`DEPTH` integer precision lost at Kafka connector ingestion — documented, unrecoverable
- Active vessel coordinates all invalid in current dataset — documented, flagged in AI engine output
- Kafka-owned seismic table name cannot be renamed — resolved via dbt `identifier:` override

---

## Team

| Member | Primary Responsibility |
|---|---|
| **Abdelrahman Maged** | Silver dbt transformation layer (all three models); co-lead AI Port Recommendation Engine |
| **Abdulrahman Mosleh** | Full real-time streaming pipeline (PostgreSQL → Debezium → Kafka → Spark → ClickHouse); Kafka-to-Snowflake connector; AI Port Recommendation Engine (co-lead) |
| **Ethar Salah** | Batch ingestion (AWS Lambda); Power BI dashboards |
| **Amira Mohamed** | Airflow DAG orchestration |
| **Alaa Mahdy** | Gold dbt models; Grafana real-time monitoring |

> All team members contributed across layers; roles reflect primary ownership.

---

## Setup & Quickstart

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Snowflake account (Student Trial or higher)
- AWS account (Lambda + S3 + EventBridge)
- dbt Core 1.11+
- Apache Airflow 2.x

### 1. Clone the Repository

```bash
git clone https://github.com/<org>/Marisight-Maritime-Risk-Operations-Analytics-Platform.git
cd Marisight-Maritime-Risk-Operations-Analytics-Platform
```

### 2. Snowflake Setup

```bash
# Run Bronze DDL scripts
snowflake/set_up_ports.sql
snowflake/set_up_vessels.sql
# Seismic table is created automatically by the Kafka connector
```

### 3. Configure dbt

```bash
cd marisight_dbt
# Edit ~/.dbt/profiles.yml with your Snowflake credentials
dbt debug          # Verify connection
dbt deps           # Install packages
dbt run            # Execute all models
dbt test           # Run data quality tests
```

### 4. Start the Streaming Stack

```bash
cd Streaming_ingestion
docker-compose up -d     # Starts Kafka, Zookeeper, Debezium, PostgreSQL

cd ../streaming-analytics
docker-compose up -d     # Starts Spark, ClickHouse, Grafana
```

### 5. Deploy Batch Lambdas

```bash
# Package and deploy via AWS Console or AWS CLI
cd batch_extract/ports
zip -r ports_lambda.zip lambda_function.py
aws lambda update-function-code --function-name marisight-ports --zip-file fileb://ports_lambda.zip

cd ../vessels
zip -r vessels_lambda.zip lambda_function.py vessel_scraper.py base_scraper.py data_quality.py
aws lambda update-function-code --function-name marisight-vessels --zip-file fileb://vessels_lambda.zip
```

### 6. Start Airflow

```bash
cd airflow
airflow db init
airflow scheduler &
airflow webserver --port 8080
# Enable DAGs: vessels_data, ports_data, seismic_data via UI
```

### 7. Run the AI Engine (manual / via Airflow)

```bash
python batch_extract/port_recommendations_engine.py
# Output written to GOLD.GOLD_PORT_RECOMMENDATIONS_V2
```

---

## Known Limitations

| Limitation | Impact | Status |
|---|---|---|
| Vessel destination coordinates mostly invalid (`IS_VALID_COORDINATES = FALSE`) | `DISTANCE_KM = NULL` in AI recommendations | Upstream data quality issue; weight redistributed |
| Seismic numeric fields limited to integer precision (`NUMBER(38,0)`) | Reduced magnitude/coordinate accuracy | Kafka connector schema inference; unrecoverable |
| dbt Core runs locally (not on managed infrastructure) | Transformation requires local machine availability | Accepted for graduation project scope |
| Airflow runs locally (not on MWAA) | Orchestration requires local machine availability | Accepted for graduation project scope |
| Snowflake Student Trial (400 credits) | Limited compute hours | Warehouse auto-suspend + XS size throughout |

---

*Built with ❤️ by the Marisight team — ITI Data Engineering Track, R2 2026*
