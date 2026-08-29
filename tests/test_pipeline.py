import csv
import sqlite3

from product_analytics.pipeline import run_pipeline


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
        metric = connection.execute("SELECT * FROM daily_product_metrics").fetchone()
    assert metric[3:8] == (1, 1, 0, 1, 19.99)


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
