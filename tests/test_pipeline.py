import csv
import json
import sqlite3

from product_analytics.__main__ import format_result
from product_analytics.pipeline import PipelineResult, initialize_warehouse, run_pipeline


HEADERS = ["event_id", "occurred_at", "user_id", "event_name", "plan", "country", "amount_usd"]


def write_source(path, rows):
    with path.open("w", newline="") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def test_pipeline_loads_valid_events_tracks_rejections_and_builds_metrics(tmp_path):
    source = tmp_path / "events.csv"
    warehouse = tmp_path / "analytics.db"
    write_source(
        source,
        [
            {
                "event_id": "evt-1",
                "occurred_at": "2026-08-01T10:00:00Z",
                "user_id": "user-1",
                "event_name": "trial_started",
                "plan": "starter",
                "country": "in",
                "amount_usd": "0",
            },
            {
                "event_id": "evt-2",
                "occurred_at": "2026-08-01T11:00:00Z",
                "user_id": "user-1",
                "event_name": "invoice_paid",
                "plan": "starter",
                "country": "in",
                "amount_usd": "19.99",
            },
            {
                "event_id": "evt-3",
                "occurred_at": "not-a-date",
                "user_id": "user-2",
                "event_name": "page_view",
                "plan": "starter",
                "country": "in",
                "amount_usd": "0",
            },
        ],
    )

    result = run_pipeline(source, warehouse)

    assert result.status == "succeeded"
    assert result.rows_read == 3
    assert result.rows_loaded == 2
    assert result.rows_rejected == 1
    with sqlite3.connect(warehouse) as connection:
        metric = connection.execute(
            """
            SELECT active_users, page_views, trials_started, subscriptions_started,
                   paid_invoices, revenue_usd
            FROM daily_product_metrics
            """
        ).fetchone()
    assert metric == (1, 0, 1, 0, 1, 19.99)


def test_pipeline_skips_a_successfully_processed_source(tmp_path):
    source = tmp_path / "events.csv"
    warehouse = tmp_path / "analytics.db"
    write_source(
        source,
        [
            {
                "event_id": "evt-1",
                "occurred_at": "2026-08-01T10:00:00Z",
                "user_id": "user-1",
                "event_name": "page_view",
                "plan": "starter",
                "country": "in",
                "amount_usd": "0",
            }
        ],
    )

    first_run = run_pipeline(source, warehouse)
    second_run = run_pipeline(source, warehouse)

    assert first_run.status == "succeeded"
    assert second_run.status == "skipped_duplicate_source"


def test_initialize_warehouse_migrates_and_backfills_page_views(tmp_path):
    warehouse = tmp_path / "analytics.db"
    with sqlite3.connect(warehouse) as connection:
        connection.executescript(
            """
            CREATE TABLE raw_events (
                event_date TEXT NOT NULL,
                plan TEXT NOT NULL,
                country TEXT NOT NULL,
                event_name TEXT NOT NULL
            );
            INSERT INTO raw_events VALUES ('2026-08-01', 'starter', 'IN', 'page_view');
            INSERT INTO raw_events VALUES ('2026-08-01', 'starter', 'IN', 'page_view');
            CREATE TABLE daily_product_metrics (
                metric_date TEXT NOT NULL,
                plan TEXT NOT NULL,
                country TEXT NOT NULL,
                active_users INTEGER NOT NULL,
                trials_started INTEGER NOT NULL,
                subscriptions_started INTEGER NOT NULL,
                paid_invoices INTEGER NOT NULL,
                revenue_usd REAL NOT NULL,
                refreshed_at TEXT NOT NULL,
                PRIMARY KEY (metric_date, plan, country)
            );
            INSERT INTO daily_product_metrics VALUES (
                '2026-08-01', 'starter', 'IN', 2, 0, 0, 0, 0, '2026-08-01T00:00:00+00:00'
            );
            """
        )

    initialize_warehouse(warehouse)

    with sqlite3.connect(warehouse) as connection:
        metric = connection.execute(
            "SELECT page_views FROM daily_product_metrics"
        ).fetchone()
    assert metric == (2,)


def test_format_result_returns_machine_readable_json():
    result = PipelineResult(
        run_id="run-123",
        status="succeeded",
        rows_read=3,
        rows_loaded=2,
        rows_rejected=1,
        rows_duplicate=0,
        affected_dates=("2026-08-01",),
    )

    assert json.loads(format_result(result)) == {
        "affected_dates": ["2026-08-01"],
        "rows_duplicate": 0,
        "rows_loaded": 2,
        "rows_read": 3,
        "rows_rejected": 1,
        "run_id": "run-123",
        "status": "succeeded",
    }
