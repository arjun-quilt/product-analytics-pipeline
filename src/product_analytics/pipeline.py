from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

REQUIRED_COLUMNS = {"event_id", "occurred_at", "user_id", "event_name", "plan", "country", "amount_usd"}
VALID_EVENT_NAMES = {"page_view", "trial_started", "subscription_started", "invoice_paid"}


class EventValidationError(ValueError):
    """Raised when a source record violates the input data contract."""


@dataclass(frozen=True)
class AnalyticsEvent:
    event_id: str
    occurred_at: str
    event_date: str
    user_id: str
    event_name: str
    plan: str
    country: str
    amount_usd: float


@dataclass(frozen=True)
class PipelineResult:
    run_id: str | None
    status: str
    rows_read: int
    rows_loaded: int
    rows_rejected: int
    rows_duplicate: int
    affected_dates: tuple[str, ...]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _connect(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_warehouse(database_path: Path) -> None:
    with _connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                source_checksum TEXT NOT NULL UNIQUE,
                source_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                rows_read INTEGER NOT NULL DEFAULT 0,
                rows_loaded INTEGER NOT NULL DEFAULT 0,
                rows_rejected INTEGER NOT NULL DEFAULT 0,
                rows_duplicate INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS raw_events (
                event_id TEXT PRIMARY KEY,
                occurred_at TEXT NOT NULL,
                event_date TEXT NOT NULL,
                user_id TEXT NOT NULL,
                event_name TEXT NOT NULL,
                plan TEXT NOT NULL,
                country TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                source_name TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rejected_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                line_number INTEGER NOT NULL,
                error_message TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS daily_product_metrics (
                metric_date TEXT NOT NULL,
                plan TEXT NOT NULL,
                country TEXT NOT NULL,
                active_users INTEGER NOT NULL,
                page_views INTEGER NOT NULL DEFAULT 0,
                trials_started INTEGER NOT NULL,
                subscriptions_started INTEGER NOT NULL,
                paid_invoices INTEGER NOT NULL,
                paying_users INTEGER NOT NULL DEFAULT 0,
                revenue_usd REAL NOT NULL,
                refreshed_at TEXT NOT NULL,
                PRIMARY KEY (metric_date, plan, country)
            );
            """
        )
        metric_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(daily_product_metrics)")
        }
        if "page_views" not in metric_columns:
            connection.execute(
                "ALTER TABLE daily_product_metrics ADD COLUMN page_views INTEGER NOT NULL DEFAULT 0"
            )
            connection.execute(
                """
                UPDATE daily_product_metrics
                SET page_views = (
                    SELECT COUNT(*)
                    FROM raw_events
                    WHERE raw_events.event_date = daily_product_metrics.metric_date
                      AND raw_events.plan = daily_product_metrics.plan
                      AND raw_events.country = daily_product_metrics.country
                      AND raw_events.event_name = 'page_view'
                )
                """
            )
        if "paying_users" not in metric_columns:
            connection.execute(
                "ALTER TABLE daily_product_metrics ADD COLUMN paying_users INTEGER NOT NULL DEFAULT 0"
            )
            connection.execute(
                """
                UPDATE daily_product_metrics
                SET paying_users = (
                    SELECT COUNT(DISTINCT user_id)
                    FROM raw_events
                    WHERE raw_events.event_date = daily_product_metrics.metric_date
                      AND raw_events.plan = daily_product_metrics.plan
                      AND raw_events.country = daily_product_metrics.country
                      AND raw_events.event_name = 'invoice_paid'
                )
                """
            )


def _checksum(source_path: Path) -> str:
    digest = hashlib.sha256()
    with source_path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(64 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_event(record: dict[str, str | None]) -> AnalyticsEvent:
    missing_values = [field for field in REQUIRED_COLUMNS if not (record.get(field) or "").strip()]
    if missing_values:
        raise EventValidationError(f"missing required values: {', '.join(sorted(missing_values))}")

    event_name = (record["event_name"] or "").strip()
    if event_name not in VALID_EVENT_NAMES:
        raise EventValidationError(f"unsupported event_name: {event_name}")

    try:
        occurred_at = datetime.fromisoformat((record["occurred_at"] or "").replace("Z", "+00:00"))
    except ValueError as error:
        raise EventValidationError("occurred_at must be an ISO 8601 timestamp") from error
    if occurred_at.tzinfo is None:
        raise EventValidationError("occurred_at must include a timezone")

    try:
        amount_usd = float(record["amount_usd"] or "")
    except ValueError as error:
        raise EventValidationError("amount_usd must be numeric") from error
    if amount_usd < 0:
        raise EventValidationError("amount_usd must be non-negative")

    normalized_time = occurred_at.astimezone(UTC)
    return AnalyticsEvent(
        event_id=(record["event_id"] or "").strip(),
        occurred_at=normalized_time.replace(microsecond=0).isoformat(),
        event_date=normalized_time.date().isoformat(),
        user_id=(record["user_id"] or "").strip(),
        event_name=event_name,
        plan=(record["plan"] or "").strip().lower(),
        country=(record["country"] or "").strip().upper(),
        amount_usd=amount_usd,
    )


def _refresh_daily_metrics(connection: sqlite3.Connection, dates: set[str]) -> None:
    refreshed_at = _now()
    for event_date in dates:
        connection.execute("DELETE FROM daily_product_metrics WHERE metric_date = ?", (event_date,))
        connection.execute(
            """
            INSERT INTO daily_product_metrics (
                metric_date, plan, country, active_users, page_views, trials_started,
                subscriptions_started, paid_invoices, paying_users, revenue_usd, refreshed_at
            )
            SELECT
                event_date,
                plan,
                country,
                COUNT(DISTINCT user_id),
                SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END),
                SUM(CASE WHEN event_name = 'trial_started' THEN 1 ELSE 0 END),
                SUM(CASE WHEN event_name = 'subscription_started' THEN 1 ELSE 0 END),
                SUM(CASE WHEN event_name = 'invoice_paid' THEN 1 ELSE 0 END),
                COUNT(DISTINCT CASE WHEN event_name = 'invoice_paid' THEN user_id END),
                ROUND(SUM(CASE WHEN event_name = 'invoice_paid' THEN amount_usd ELSE 0 END), 2),
                ?
            FROM raw_events
            WHERE event_date = ?
            GROUP BY event_date, plan, country
            """,
            (refreshed_at, event_date),
        )


def run_pipeline(source_path: str | Path, database_path: str | Path) -> PipelineResult:
    """Ingest a CSV file and incrementally update the product-metrics mart.

    The source checksum avoids rerunning an already-successful file, while the
    `event_id` primary key protects the raw layer when files overlap.
    """

    source = Path(source_path)
    database = Path(database_path)
    if not source.exists():
        raise FileNotFoundError(source)

    initialize_warehouse(database)
    source_checksum = _checksum(source)
    with _connect(database) as connection:
        existing_run = connection.execute(
            "SELECT run_id FROM pipeline_runs WHERE source_checksum = ? AND status = 'succeeded'",
            (source_checksum,),
        ).fetchone()
    if existing_run:
        return PipelineResult(None, "skipped_duplicate_source", 0, 0, 0, 0, ())

    run_id = str(uuid4())
    with _connect(database) as connection:
        connection.execute(
            """
            INSERT INTO pipeline_runs (run_id, source_checksum, source_name, status, started_at)
            VALUES (?, ?, ?, 'running', ?)
            """,
            (run_id, source_checksum, source.name, _now()),
        )

    rows_read = rows_loaded = rows_rejected = rows_duplicate = 0
    affected_dates: set[str] = set()
    try:
        with source.open(newline="", encoding="utf-8") as source_file, _connect(database) as connection:
            reader = csv.DictReader(source_file)
            header_names = reader.fieldnames or []
            actual_columns = set(header_names)
            duplicate_columns = sorted(
                column for column in actual_columns if header_names.count(column) > 1
            )
            if duplicate_columns:
                raise EventValidationError(
                    f"source has duplicate columns: {', '.join(duplicate_columns)}"
                )
            missing_columns = REQUIRED_COLUMNS - actual_columns
            if missing_columns:
                raise EventValidationError(f"source is missing columns: {', '.join(sorted(missing_columns))}")

            for line_number, record in enumerate(reader, start=2):
                rows_read += 1
                try:
                    event = _parse_event(record)
                except EventValidationError as error:
                    rows_rejected += 1
                    connection.execute(
                        """
                        INSERT INTO rejected_events (run_id, line_number, error_message, raw_payload, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (run_id, line_number, str(error), json.dumps(record), _now()),
                    )
                    continue

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO raw_events (
                        event_id, occurred_at, event_date, user_id, event_name,
                        plan, country, amount_usd, source_name, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.occurred_at,
                        event.event_date,
                        event.user_id,
                        event.event_name,
                        event.plan,
                        event.country,
                        event.amount_usd,
                        source.name,
                        _now(),
                    ),
                )
                if cursor.rowcount == 0:
                    rows_duplicate += 1
                else:
                    rows_loaded += 1
                    affected_dates.add(event.event_date)

            _refresh_daily_metrics(connection, affected_dates)
            connection.execute(
                """
                UPDATE pipeline_runs
                SET status = 'succeeded', completed_at = ?, rows_read = ?, rows_loaded = ?,
                    rows_rejected = ?, rows_duplicate = ?
                WHERE run_id = ?
                """,
                (_now(), rows_read, rows_loaded, rows_rejected, rows_duplicate, run_id),
            )
    except Exception as error:
        with _connect(database) as connection:
            connection.execute(
                "UPDATE pipeline_runs SET status = 'failed', completed_at = ?, error_message = ? WHERE run_id = ?",
                (_now(), str(error), run_id),
            )
        raise

    return PipelineResult(
        run_id=run_id,
        status="succeeded",
        rows_read=rows_read,
        rows_loaded=rows_loaded,
        rows_rejected=rows_rejected,
        rows_duplicate=rows_duplicate,
        affected_dates=tuple(sorted(affected_dates)),
    )
