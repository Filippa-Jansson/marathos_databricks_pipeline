# Marathos Databricks Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?logo=delta&logoColor=white)

An end-to-end data pipeline in Databricks for Marathos, a fictional company that hosts marathons around the world. The pipeline turns raw ultramarathon results into a star schema, a dashboard and a Genie chatbot that business stakeholders can ask questions to. Built as part of the course Big Data and Cloud – Practical Development and NoSQL in the Data Engineering program at STI.

## Data

*Two Centuries of UM Races*: results from ultramarathon events over two centuries, with event names, distances, dates, athlete performance, country, age category and average speed.

## Architecture

The pipeline follows the **medallion architecture** and is built with **Lakeflow Declarative Pipelines** in Unity Catalog.

```
CSV in volume  ──►  Bronze  ──►  Silver  ──►  Gold  ──►  Dashboard + Genie
(marathos.default.raw)   raw data    cleaned OBT   star schema
```

| Layer | Table | Description |
|---|---|---|
| Bronze | `results_raw` | Raw CSV ingested as a streaming table, unchanged from the source |
| Silver | `marathos_obt` | One big table with cleaned and validated data |
| Gold | `fct_results`, `dim_athlete`, `dim_event` | Star schema for analysis |
| Gold | `mart_event_summary`, `mart_top_countries` | Aggregated views for the dashboard |

## Data cleaning (silver)

- Renamed all columns to `snake_case`
- Classified events as `distance` (km/mi) or `time` (h)
- Removed invalid results, such as performances in days, stage races and distance events without a time
- Filled missing clubs and age categories with `Unknown` and removed stray `*` characters
- Kept only realistic average speeds (1–25 km/h) and birth years
- Extracted the event date from date ranges like `01.06.2019-02.06.2019`
- Removed events measured in days, which have an unknown event type
- Created `event_id` and `result_id` keys with 64-bit hashing (`xxhash64`) and removed exact duplicate results

The reasoning behind each step is documented in [`explorations/eda_silver.ipynb`](explorations/eda_silver.ipynb).

## Data quality findings

Checking that every key in the gold layer was unique revealed several issues, which were fixed step by step:

| Issue | Fix |
|---|---|
| The same event held every year got the same id, which inflated result counts in the dashboard (110 million instead of 6 million) | Built `event_id` from name, date, year, distance and number of finishers |
| Several distances on the same day (e.g. 50 km and 100 km) shared an id | Included the distance in `event_id` |
| Different events and results got the same id because 32-bit `hash()` collided | Switched to 64-bit `xxhash64()` |
| `dim_athlete` had several rows per athlete, since age category and club change over time | One row per athlete with the latest values, and age category moved to the fact table |
| The source data contains exact duplicate results, and some different athletes share the same `athlete_id` | Removed exact duplicates and added birth year and club to `result_id` |

The last issue cannot be fully solved without better source data, so a few dozen of 1.2 million athletes may be merged in `dim_athlete`.

## Dimensional model

`fct_results` holds one row per athlete result and links to `dim_athlete` and `dim_event`. See [`dimensional_modeling/marathos_dimensional_model.pdf`](dimensional_modeling/marathos_dimensional_model.pdf).

## Dashboard and Genie

The gold layer feeds a Databricks dashboard and the **Marathon Athlete and Event Analytics** Genie space, where stakeholders can ask questions in plain language. To make sure Genie's answers are reliable, they were checked against manual SQL queries in [`explorations/genie_verify.dbquery.ipynb`](explorations/genie_verify.dbquery.ipynb).

## Project structure

```
├── setup_catalog.ipynb        # creates the catalog, schemas and volume
├── transformations/
│   ├── bronze/                # ingestion
│   ├── silver/                # cleaning, one big table
│   └── gold/                  # star schema and marts
├── explorations/              # EDA, Genie verification and cleanup queries
└── dimensional_modeling/      # dimensional model (PDF)
```

## How to run

1. Run `setup_catalog.ipynb` to create the `marathos` catalog, schemas and volume
2. Upload the dataset to `/Volumes/marathos/default/raw`
3. Create a Lakeflow pipeline with the `transformations/` folder as source and run it

## Lessons learned and next steps

The raw data had many small quality issues, and one of the main lessons was to prioritize the issues with the biggest impact on the analysis instead of trying to fix everything at once. Possible next steps:

- Add a date dimension and country data with full country names
- Schedule the pipeline to run automatically
- Expand the dashboard with more insights