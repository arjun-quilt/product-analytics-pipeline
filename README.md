# Product Analytics Pipeline

[![Tests](https://github.com/arjun-quilt/product-analytics-pipeline/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/arjun-quilt/product-analytics-pipeline/actions/workflows/tests.yml)

An incremental ELT pipeline that turns raw product events into a daily analytics mart. It uses only the Python standard library and SQLite so it can be run and tested locally, while keeping the same concerns as a warehouse pipeline: input contracts, idempotency, rejected-record handling, and repeatable metric models.

## Architecture

```text
CSV event source
   │
   ├── validate fields, timestamp, event type, and amount
   ├── raw_events (deduplicated by event_id)
   ├── rejected_events (line number + reason + original payload)
   └── daily_product_metrics (incrementally refreshed semantic layer)
```

The source file checksum prevents a successful source from being loaded twice. The raw event primary key protects the pipeline again when files overlap, and duplicate CSV column names fail the run before records are loaded. SQLite indexes speed up daily metric refreshes and rejected-record lookups.

## Run

```bash
python -m pip install -e .
product-analytics \
  --source data/raw/product_events.csv \
  --database data/analytics.db
```

The command prints a JSON run summary, which makes it easy to capture row
counts and the affected dates in an orchestrator or CI log.

## Query the resulting mart

```bash
sqlite3 data/analytics.db \
  "SELECT metric_date, plan, country, active_users, page_views, paying_users, revenue_usd FROM daily_product_metrics;"
```

## Test

With Python 3.11 or newer, create a virtual environment and install the test dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[test]"
python -m pytest
```

GitHub Actions runs the test suite and the installed CLI against the sample events
on Python 3.11, 3.12, 3.13, and 3.14 for every push to `main` and every pull request
targeting `main`. You can also run the **Tests** workflow manually from the
repository's Actions tab.

## Production extension points

- replace the CSV reader with an object-store or message-queue source;
- write raw and curated models to a warehouse such as BigQuery or Snowflake;
- orchestrate the command from Prefect, Dagster, or Airflow; and
- emit run metrics and rejected-record counts to the team’s monitoring platform.
