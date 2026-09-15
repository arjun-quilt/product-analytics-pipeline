# Product Analytics Pipeline

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

The source file checksum prevents a successful source from being loaded twice. The raw event primary key protects the pipeline again when files overlap.

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
  "SELECT metric_date, plan, country, active_users, revenue_usd FROM daily_product_metrics;"
```

## Test

```bash
pytest
```

## Production extension points

- replace the CSV reader with an object-store or message-queue source;
- write raw and curated models to a warehouse such as BigQuery or Snowflake;
- orchestrate the command from Prefect, Dagster, or Airflow; and
- emit run metrics and rejected-record counts to the team’s monitoring platform.
