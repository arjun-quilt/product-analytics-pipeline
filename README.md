# Product Analytics Pipeline

[![Tests](https://github.com/arjun-quilt/product-analytics-pipeline/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/arjun-quilt/product-analytics-pipeline/actions/workflows/tests.yml)

Turn product events into daily engagement and revenue metrics with Python and SQLite. This local batch pipeline demonstrates input validation, deduplication, rejected-record handling, incremental aggregation, and operational run tracking without a cloud account or third-party runtime dependencies.

The included sample contains five synthetic events. It produces three daily metric rows and skips an identical second load, making the project easy to run and inspect locally.

## Architecture

```text
CSV source → checksum + header validation → row validation
                                             ├── rejected_events
                                             └── raw_events → daily_product_metrics

pipeline_runs records source status, timestamps, counters, and failures.
```

Raw events are deduplicated by `event_id`. Only dates receiving new events are reaggregated, using all stored events for each affected date. Indexes support date-based aggregation and rejected-record lookups by run.

## Quick start

Requirements: Python 3.11 or newer and Git. The SQL examples also use the `sqlite3` command-line program; the pipeline itself uses Python's built-in SQLite module. These shell commands work on macOS and Linux.

```bash
git clone https://github.com/arjun-quilt/product-analytics-pipeline.git
cd product-analytics-pipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .

product-analytics \
  --source data/raw/product_events.csv \
  --database data/analytics.db | python -m json.tool
```

On Windows, activate the environment with `.venv\Scripts\Activate.ps1` in PowerShell. You can also invoke the installed command as `python -m product_analytics`.

For a new database, the JSON summary is shown below. `run_id` is a generated UUID and will differ on each new source; the other values match the included sample. The CLI emits one JSON line, and `python -m json.tool` formats it for display.

```json
{
    "affected_dates": ["2026-08-01", "2026-08-02"],
    "rows_duplicate": 0,
    "rows_loaded": 5,
    "rows_read": 5,
    "rows_rejected": 0,
    "run_id": "<generated UUID>",
    "status": "succeeded"
}
```

Run the same command again against the same database to verify source deduplication:

```json
{
    "affected_dates": [],
    "rows_duplicate": 0,
    "rows_loaded": 0,
    "rows_read": 0,
    "rows_rejected": 0,
    "run_id": null,
    "status": "skipped_duplicate_source"
}
```

The source checksum covers file contents, so renaming an unchanged file does not cause a reload. To repeat a clean demo, use a new `--database` path.

## Query the resulting mart

```bash
sqlite3 -header -column data/analytics.db \
  "SELECT metric_date, plan, country, active_users, page_views, paying_users, revenue_usd
   FROM daily_product_metrics
   ORDER BY metric_date, plan, country;"
```

Expected results for the included sample:

| metric_date | plan | country | active_users | page_views | paying_users | revenue_usd |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-08-01 | growth | US | 1 | 0 | 1 | 49.0 |
| 2026-08-01 | starter | IN | 1 | 1 | 0 | 0.0 |
| 2026-08-02 | starter | GB | 1 | 1 | 0 | 0.0 |

Each row represents one UTC date, normalized plan, and normalized country:

| Metric | Definition within that group |
| --- | --- |
| `active_users` | Distinct users with any accepted event |
| `page_views` | Number of `page_view` events |
| `trials_started` | Number of `trial_started` events |
| `subscriptions_started` | Number of `subscription_started` events |
| `paid_invoices` | Number of `invoice_paid` events |
| `paying_users` | Distinct users with an `invoice_paid` event, including zero-value invoices |
| `revenue_usd` | Sum of `amount_usd` for `invoice_paid` events, rounded to two decimal places |

Distinct-user counts are scoped to each group. Summing them across plans, countries, or dates can count the same user more than once.

## Input contract

Use UTF-8 CSV with a header row. All seven columns below must appear exactly once and have nonempty values. Header names and event names are case-sensitive; additional unique columns are ignored.

| Column | Rule |
| --- | --- |
| `event_id` | Identifier used to deduplicate events across all source files |
| `occurred_at` | ISO 8601 timestamp with a timezone, such as `2026-08-01T08:00:00Z`; converted to UTC |
| `user_id` | User identifier used for distinct-user metrics |
| `event_name` | One of `page_view`, `trial_started`, `subscription_started`, `invoice_paid` |
| `plan` | Nonempty label, normalized to lowercase |
| `country` | Nonempty label, normalized to uppercase; country codes are not checked against a reference list |
| `amount_usd` | Finite, non-negative number; use `0` for events without a monetary amount |

Missing or duplicate headers fail the source before any events are loaded. Invalid event rows are stored in `rejected_events` with the record position, reason, and original payload; valid rows continue through the pipeline. A run can therefore succeed with rejected rows, including when every row is rejected. Inspect `rows_rejected` to assess data quality.

## Incremental loads and failure handling

- Successfully processed source contents are skipped on subsequent runs. Valid events repeated in different files are counted as duplicates; the first stored event wins.
- New events for an older date trigger a rebuild of that date's metrics. Existing databases automatically gain and backfill the `page_views` and `paying_users` columns from retained raw events.
- After a run starts, a processing exception rolls back its event inserts, rejected records, and metric changes, then records the failure separately.
- A source recorded as `failed` can be retried with the same command after resolving the cause. The retry reuses its run ID and audit row, resetting the previous attempt's status, counters, and error. Successful sources remain skipped.
- A source already marked `running` is not taken over. An interrupted process can leave that status behind; inspect it before manual recovery.

Inspect run status and counters:

```bash
sqlite3 -header -column data/analytics.db \
  "SELECT run_id, source_name, status, rows_read, rows_loaded,
          rows_rejected, rows_duplicate, error_message
   FROM pipeline_runs
   ORDER BY started_at, run_id;"
```

Inspect rejected records, using a run ID from the previous query:

```bash
sqlite3 -header -column data/analytics.db \
  "SELECT line_number, error_message, raw_payload
   FROM rejected_events
   WHERE run_id = '<run_id>'
   ORDER BY line_number;"
```

The sample file contains only valid rows, so its rejected-record query is empty. Correct rejected rows in a new source file to process them; rerunning identical successful source contents will skip them.

## Tests and CI

In the virtual environment from the quick start:

```bash
python -m pip install -e ".[test]"
python -m pytest
```

Tests cover event ingestion and rejection, source deduplication, duplicate headers, non-finite amounts, rollback and retry behavior, unfinished-run protection, schema backfills, indexes, and JSON output.

GitHub Actions runs the tests and the installed CLI against the sample events on Python 3.11, 3.12, 3.13, and 3.14 for every push to `main` and every pull request targeting `main`. The **Tests** workflow can also be run manually from the repository's Actions tab.

## Scope and limitations

This is a local batch analytics portfolio project. It deliberately keeps infrastructure small so the data flow, SQL, and failure handling are easy to inspect.

- Run one ingestion process per database. Distributed scheduling, automatic crash recovery, and concurrent schema migrations are outside the current scope.
- Events are append-only. Changing an existing `event_id` does not update its stored values; refunds, event corrections, and currency conversion are not modeled.
- Amounts use floating-point arithmetic for analytical summaries. An accounting system would need exact decimal or integer-minor-unit storage and additional business rules.
- Run tracking retains one audit row per source checksum, representing the latest attempt. It does not preserve an attempt-by-attempt history.
- CSV record positions are reported as `line_number`; embedded newlines in quoted fields mean these may differ from physical file line numbers.

## Possible extensions

For a larger deployment, the same concepts could be extended with an object-store source, a managed warehouse, scheduled orchestration, and alerts based on run failures or rejected-row counts. These integrations are not required to run this project.
