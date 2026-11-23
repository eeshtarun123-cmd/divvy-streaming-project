import os
import hashlib
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = BASE_DIR / "logs"
WAREHOUSE_PATH = WAREHOUSE_DIR / "divvy.duckdb"
LOG_PATH = LOG_DIR / "pipeline.log"


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_utc_datetime(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc).replace(tzinfo=None)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def configure_logging(name: str = "divvy_pipeline") -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        file_handler = logging.FileHandler(LOG_PATH)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def get_connection(read_only: bool = False):
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=read_only)
    if os.environ.get("DIVVY_SCHEDULED_LOAD") == "1":
        con.execute("SET memory_limit = '3GB'")
        con.execute("SET threads = 2")
    else:
        con.execute("PRAGMA threads=4")
    if not read_only:
        initialize_warehouse(con)
    return con


def initialize_warehouse(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS mart")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
            run_id VARCHAR,
            step_name VARCHAR,
            layer_name VARCHAR,
            source_name VARCHAR,
            target_table VARCHAR,
            status VARCHAR,
            rows_affected BIGINT,
            started_at_utc TIMESTAMP,
            ended_at_utc TIMESTAMP,
            duration_seconds DOUBLE,
            message VARCHAR,
            error_message VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.validation_results (
            run_id VARCHAR,
            validation_name VARCHAR,
            layer_name VARCHAR,
            table_name VARCHAR,
            severity VARCHAR,
            status VARCHAR,
            failed_count BIGINT,
            threshold VARCHAR,
            message VARCHAR,
            checked_at_utc TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.source_file_manifest (
            source_name VARCHAR,
            source_file_path VARCHAR,
            source_file_name VARCHAR,
            file_size_bytes BIGINT,
            file_modified_at TIMESTAMP,
            file_checksum VARCHAR,
            processed_status VARCHAR,
            processed_at_utc TIMESTAMP,
            row_count BIGINT,
            min_event_time TIMESTAMP,
            max_event_time TIMESTAMP,
            run_id VARCHAR,
            message VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_watermarks (
            source_name VARCHAR,
            source_type VARCHAR,
            latest_ingested_at_utc TIMESTAMP,
            latest_feed_last_updated_at_utc TIMESTAMP,
            latest_trip_started_at TIMESTAMP,
            latest_trip_ended_at TIMESTAMP,
            latest_source_file_period VARCHAR,
            latest_processed_file VARCHAR,
            latest_source_file_path VARCHAR,
            updated_at_utc TIMESTAMP,
            run_id VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW pipeline_run_log AS
        SELECT *
        FROM ops.pipeline_run_log
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW validation_results AS
        SELECT *
        FROM ops.validation_results
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW source_file_manifest AS
        SELECT *
        FROM ops.source_file_manifest
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW pipeline_watermarks AS
        SELECT *
        FROM ops.pipeline_watermarks
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW ops.validation_run_summary AS
        SELECT
            run_id,
            layer_name,
            COUNT(*) AS total_checks,
            COUNT_IF(severity = 'ERROR' AND status = 'FAIL') AS error_failures,
            COUNT_IF(status = 'WARNING') AS warning_count,
            COUNT_IF(status = 'PASS') AS passed_checks,
            CASE
                WHEN COUNT_IF(severity = 'ERROR' AND status = 'FAIL') > 0 THEN 'FAILED'
                WHEN COUNT_IF(status = 'WARNING') > 0 THEN 'PASSED_WITH_WARNINGS'
                ELSE 'PASSED'
            END AS run_status,
            MIN(checked_at_utc) AS first_checked_at_utc,
            MAX(checked_at_utc) AS last_checked_at_utc
        FROM ops.validation_results
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW validation_run_summary AS
        SELECT *
        FROM ops.validation_run_summary
        """
    )


def make_run_id() -> str:
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def record_pipeline_event(
    con,
    run_id: str,
    step_name: str,
    layer_name: str,
    source_name: str,
    target_table: str,
    status: str,
    rows_affected=None,
    started_at_utc=None,
    ended_at_utc=None,
    duration_seconds=None,
    message=None,
    error_message=None,
) -> None:
    initialize_warehouse(con)
    con.execute(
        """
        INSERT INTO ops.pipeline_run_log
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            step_name,
            layer_name,
            source_name,
            target_table,
            status,
            rows_affected,
            started_at_utc,
            ended_at_utc,
            duration_seconds,
            message,
            error_message,
        ],
    )


def record_validation_result(
    con,
    run_id: str,
    validation_name: str,
    layer_name: str,
    table_name: str,
    severity: str,
    status: str,
    failed_count: int,
    threshold,
    message: str,
) -> None:
    initialize_warehouse(con)
    con.execute(
        """
        INSERT INTO ops.validation_results
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            validation_name,
            layer_name,
            table_name,
            severity,
            status,
            failed_count,
            str(threshold),
            message,
            utc_now_naive(),
        ],
    )


def table_exists(con, qualified_table_name: str) -> bool:
    if "." in qualified_table_name:
        schema_name, table_name = qualified_table_name.split(".", 1)
    else:
        schema_name, table_name = "main", qualified_table_name
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            """,
            [schema_name, table_name],
        ).fetchone()[0]
        > 0
    )


def file_checksum(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def source_file_record(source_name: str, path: Path) -> dict:
    stat = path.stat()
    return {
        "source_name": source_name,
        "source_file_path": str(path),
        "source_file_name": path.name,
        "file_size_bytes": stat.st_size,
        "file_modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).replace(tzinfo=None),
        "file_checksum": file_checksum(path),
    }


def get_existing_manifest_records(con, paths) -> dict:
    path_values = [str(path) for path in paths]
    if not path_values:
        return {}
    placeholders = ", ".join(["?"] * len(path_values))
    rows = con.execute(
        f"""
        SELECT
            source_file_path,
            file_size_bytes,
            file_modified_at,
            file_checksum,
            processed_status,
            row_count,
            min_event_time,
            max_event_time,
            message
        FROM ops.source_file_manifest
        WHERE source_file_path IN ({placeholders})
        """,
        path_values,
    ).fetchall()
    return {
        row[0]: {
            "file_size_bytes": row[1],
            "file_modified_at": row[2],
            "file_checksum": row[3],
            "processed_status": row[4],
            "row_count": row[5],
            "min_event_time": row[6],
            "max_event_time": row[7],
            "message": row[8],
        }
        for row in rows
    }


def source_file_has_changed(existing_record, new_record: dict) -> bool:
    if existing_record is None:
        return True
    if existing_record.get("processed_status") not in ("PROCESSED", "SKIPPED_UNCHANGED"):
        return True
    return (
        existing_record.get("file_size_bytes") != new_record.get("file_size_bytes")
        or existing_record.get("file_checksum") != new_record.get("file_checksum")
    )


def upsert_source_file_manifest(
    con,
    record: dict,
    processed_status: str,
    run_id: str,
    row_count=None,
    min_event_time=None,
    max_event_time=None,
    message=None,
) -> None:
    initialize_warehouse(con)
    con.execute(
        "DELETE FROM ops.source_file_manifest WHERE source_file_path = ?",
        [record["source_file_path"]],
    )
    con.execute(
        """
        INSERT INTO ops.source_file_manifest
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            record["source_name"],
            record["source_file_path"],
            record["source_file_name"],
            record["file_size_bytes"],
            record["file_modified_at"],
            record["file_checksum"],
            processed_status,
            utc_now_naive(),
            row_count,
            min_event_time,
            max_event_time,
            run_id,
            message,
        ],
    )


def mark_source_file_skipped(con, record: dict, existing_record: dict, run_id: str) -> None:
    upsert_source_file_manifest(
        con,
        record,
        processed_status="SKIPPED_UNCHANGED",
        run_id=run_id,
        row_count=existing_record.get("row_count"),
        min_event_time=existing_record.get("min_event_time"),
        max_event_time=existing_record.get("max_event_time"),
        message="Unchanged since previous processed run",
    )


def upsert_pipeline_watermark(
    con,
    source_name: str,
    source_type: str,
    run_id: str,
    latest_ingested_at_utc=None,
    latest_feed_last_updated_at_utc=None,
    latest_trip_started_at=None,
    latest_trip_ended_at=None,
    latest_source_file_period=None,
    latest_processed_file=None,
    latest_source_file_path=None,
) -> None:
    initialize_warehouse(con)
    con.execute("DELETE FROM ops.pipeline_watermarks WHERE source_name = ?", [source_name])
    con.execute(
        """
        INSERT INTO ops.pipeline_watermarks
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            source_name,
            source_type,
            latest_ingested_at_utc,
            latest_feed_last_updated_at_utc,
            latest_trip_started_at,
            latest_trip_ended_at,
            latest_source_file_period,
            latest_processed_file,
            latest_source_file_path,
            utc_now_naive(),
            run_id,
        ],
    )


def run_logged_step(
    con,
    run_id: str,
    step_name: str,
    layer_name: str,
    source_name: str,
    target_table: str,
    func,
    logger=None,
):
    logger = logger or configure_logging()
    started_at = utc_now_naive()
    start_time = time.perf_counter()
    logger.info("Starting step=%s target=%s", step_name, target_table)
    record_pipeline_event(
        con=con,
        run_id=run_id,
        step_name=step_name,
        layer_name=layer_name,
        source_name=source_name,
        target_table=target_table,
        status="STARTED",
        started_at_utc=started_at,
    )

    try:
        result = func()
        rows = result.get("rows_affected") if isinstance(result, dict) else None
        message = result.get("message") if isinstance(result, dict) else None
        ended_at = utc_now_naive()
        duration = time.perf_counter() - start_time
        record_pipeline_event(
            con=con,
            run_id=run_id,
            step_name=step_name,
            layer_name=layer_name,
            source_name=source_name,
            target_table=target_table,
            status="SUCCESS",
            rows_affected=rows,
            started_at_utc=started_at,
            ended_at_utc=ended_at,
            duration_seconds=duration,
            message=message,
        )
        logger.info("Finished step=%s rows=%s duration=%.2fs", step_name, rows, duration)
        return result
    except Exception as exc:
        ended_at = utc_now_naive()
        duration = time.perf_counter() - start_time
        record_pipeline_event(
            con=con,
            run_id=run_id,
            step_name=step_name,
            layer_name=layer_name,
            source_name=source_name,
            target_table=target_table,
            status="FAILED",
            started_at_utc=started_at,
            ended_at_utc=ended_at,
            duration_seconds=duration,
            error_message=str(exc),
        )
        logger.exception("Failed step=%s", step_name)
        raise


def count_table(con, table_name: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def run_sql_file(con, sql_path: Path) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    con.execute(sql)
