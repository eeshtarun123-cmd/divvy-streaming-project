import argparse
from pathlib import Path

from export_parquet_layers import export_all_layers, export_table_to_parquet
from ingest_gbfs_live import fetch_gbfs_snapshots, ingest_gbfs_raw_files
from ingest_historical_trips import ingest_historical_trips
from pipeline_common import (
    BASE_DIR,
    DATA_DIR,
    PROCESSED_DIR,
    RAW_DIR,
    configure_logging,
    get_connection,
    make_run_id,
    run_logged_step,
    run_sql_file,
)
from transform_marts import run_mart_transforms
from transform_staging import run_staging_transforms
from validate_pipeline import validate_layer


def refresh_pipeline_health():
    con = get_connection()
    try:
        run_sql_file(con, BASE_DIR / "sql" / "marts" / "070_pipeline_health.sql")
    finally:
        con.close()
    return {"rows_affected": 1, "message": "Refreshed mart.mart_pipeline_health"}


def run_pipeline(
    fetch_live: bool = False,
    trip_input_dir: Path = DATA_DIR,
    output_root: Path = PROCESSED_DIR,
    full_refresh: bool = False,
):
    logger = configure_logging()
    run_id = make_run_id()
    con = get_connection()
    logger.info("Pipeline run_id=%s", run_id)

    if fetch_live:
        run_logged_step(
            con,
            run_id,
            "fetch_gbfs_snapshots",
            "raw",
            "GBFS API",
            str(RAW_DIR),
            lambda: {"rows_affected": len(fetch_gbfs_snapshots(RAW_DIR)), "message": "Fetched fresh GBFS snapshots"},
            logger,
        )

    run_logged_step(
        con,
        run_id,
        "ingest_historical_trips",
        "raw",
        str(trip_input_dir),
        "raw.historical_trips",
        lambda: ingest_historical_trips(trip_input_dir, full_refresh=full_refresh, run_id=run_id),
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "ingest_gbfs_live",
        "raw",
        str(RAW_DIR),
        "raw.gbfs_*",
        lambda: ingest_gbfs_raw_files(RAW_DIR, full_refresh=full_refresh, run_id=run_id),
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "validate_raw",
        "raw",
        "raw.*",
        "ops.validation_results",
        lambda: validate_layer("raw", run_id=run_id),
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "transform_staging",
        "staging",
        "raw.*",
        "staging.*",
        run_staging_transforms,
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "validate_staging",
        "staging",
        "staging.*",
        "ops.validation_results",
        lambda: validate_layer("staging", run_id=run_id),
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "transform_marts",
        "mart",
        "staging.*",
        "mart.*",
        run_mart_transforms,
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "validate_marts",
        "mart",
        "mart.*",
        "ops.validation_results",
        lambda: validate_layer("mart", run_id=run_id),
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "refresh_pipeline_health",
        "mart",
        "ops.*",
        "mart.mart_pipeline_health",
        refresh_pipeline_health,
        logger,
    )
    run_logged_step(
        con,
        run_id,
        "export_parquet_layers",
        "processed",
        "raw/staging/mart",
        str(output_root),
        lambda: export_all_layers(output_root),
        logger,
    )

    refresh_pipeline_health()
    export_table_to_parquet("mart", "mart_pipeline_health", output_root)
    con.close()
    logger.info("Pipeline complete run_id=%s", run_id)
    return run_id


def main():
    parser = argparse.ArgumentParser(description="Run the Divvy analytics pipeline end to end.")
    parser.add_argument("--fetch-live", action="store_true", help="Fetch a fresh GBFS snapshot before loading raw files.")
    parser.add_argument("--full-refresh", action="store_true", help="Rebuild raw tables from all source files.")
    parser.add_argument("--trip-input-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--output-root", type=Path, default=PROCESSED_DIR)
    args = parser.parse_args()
    run_pipeline(
        fetch_live=args.fetch_live,
        trip_input_dir=args.trip_input_dir,
        output_root=args.output_root,
        full_refresh=args.full_refresh,
    )


if __name__ == "__main__":
    main()
