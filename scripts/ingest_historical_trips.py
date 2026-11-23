import argparse
from pathlib import Path
from zipfile import ZipFile

from pipeline_common import (
    DATA_DIR,
    RAW_DIR,
    configure_logging,
    count_table,
    get_existing_manifest_records,
    get_connection,
    mark_source_file_skipped,
    make_run_id,
    record_pipeline_event,
    source_file_has_changed,
    source_file_record,
    table_exists,
    upsert_pipeline_watermark,
    upsert_source_file_manifest,
    utc_now_naive,
)


def _duckdb_file_list(paths):
    quoted = []
    for path in paths:
        escaped = str(path).replace("'", "''")
        quoted.append(f"'{escaped}'")
    return "[" + ", ".join(quoted) + "]"


def _prepare_historical_trip_files(input_dir: Path):
    files = sorted(input_dir.glob("*-divvy-tripdata.csv"))
    zip_files = sorted(input_dir.glob("*-divvy-tripdata.zip"))
    extract_dir = RAW_DIR / "tripdata"
    extract_dir.mkdir(parents=True, exist_ok=True)

    for zip_path in zip_files:
        with ZipFile(zip_path) as archive:
            for member in archive.infolist():
                member_name = Path(member.filename).name
                if not member_name.endswith(".csv") or "divvy-tripdata" not in member_name:
                    continue
                output_path = extract_dir / f"{zip_path.stem}__{member_name}"
                if output_path.exists() and output_path.stat().st_size == member.file_size:
                    files.append(output_path)
                    continue
                with archive.open(member) as source, output_path.open("wb") as target:
                    target.write(source.read())
                files.append(output_path)

    return sorted(set(files))


def _ensure_historical_tables(con, full_refresh: bool = False):
    if full_refresh:
        con.execute("DROP TABLE IF EXISTS raw.historical_trip_file_profile")
        con.execute("DROP TABLE IF EXISTS raw.historical_trips")
        con.execute("DELETE FROM ops.source_file_manifest WHERE source_name = 'historical_trips'")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.historical_trips (
            source_file_name VARCHAR,
            source_file_path VARCHAR,
            source_file_period VARCHAR,
            loaded_at_utc TIMESTAMP,
            ride_id VARCHAR,
            rideable_type VARCHAR,
            started_at VARCHAR,
            ended_at VARCHAR,
            start_station_name VARCHAR,
            start_station_id VARCHAR,
            end_station_name VARCHAR,
            end_station_id VARCHAR,
            start_lat VARCHAR,
            start_lng VARCHAR,
            end_lat VARCHAR,
            end_lng VARCHAR,
            member_casual VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.historical_trip_file_profile (
            source_file_name VARCHAR,
            source_file_path VARCHAR,
            source_file_period VARCHAR,
            row_count BIGINT,
            min_started_at TIMESTAMP,
            max_started_at TIMESTAMP,
            min_ended_at TIMESTAMP,
            max_ended_at TIMESTAMP,
            distinct_ride_count BIGINT,
            duplicate_ride_row_count BIGINT,
            missing_start_station_id_count BIGINT,
            missing_end_station_id_count BIGINT,
            non_positive_duration_count BIGINT,
            over_24h_trip_count BIGINT
        )
        """
    )


def _delete_source_paths(con, paths):
    for path in paths:
        con.execute("DELETE FROM raw.historical_trips WHERE source_file_path = ?", [str(path)])
        con.execute("DELETE FROM raw.historical_trip_file_profile WHERE source_file_path = ?", [str(path)])


def _insert_historical_profiles(con, paths):
    if not paths:
        return
    placeholders = ", ".join(["?"] * len(paths))
    con.execute(
        f"""
        INSERT INTO raw.historical_trip_file_profile
        SELECT
            source_file_name,
            source_file_path,
            source_file_period,
            COUNT(*) AS row_count,
            MIN(try_cast(started_at AS TIMESTAMP)) AS min_started_at,
            MAX(try_cast(started_at AS TIMESTAMP)) AS max_started_at,
            MIN(try_cast(ended_at AS TIMESTAMP)) AS min_ended_at,
            MAX(try_cast(ended_at AS TIMESTAMP)) AS max_ended_at,
            COUNT(DISTINCT ride_id) AS distinct_ride_count,
            COUNT(*) - COUNT(DISTINCT ride_id) AS duplicate_ride_row_count,
            COUNT_IF(nullif(trim(start_station_id), '') IS NULL) AS missing_start_station_id_count,
            COUNT_IF(nullif(trim(end_station_id), '') IS NULL) AS missing_end_station_id_count,
            COUNT_IF(try_cast(ended_at AS TIMESTAMP) <= try_cast(started_at AS TIMESTAMP)) AS non_positive_duration_count,
            COUNT_IF(
                date_diff(
                    'second',
                    try_cast(started_at AS TIMESTAMP),
                    try_cast(ended_at AS TIMESTAMP)
                ) / 60.0 >= 1440
            ) AS over_24h_trip_count
        FROM raw.historical_trips
        WHERE source_file_path IN ({placeholders})
        GROUP BY 1, 2, 3
        """,
        [str(path) for path in paths],
    )


def _update_historical_manifest(con, run_id: str, file_records: dict):
    rows = con.execute(
        """
        SELECT
            source_file_path,
            row_count,
            min_started_at,
            max_ended_at,
            source_file_period
        FROM raw.historical_trip_file_profile
        """
    ).fetchall()
    profile_by_path = {
        row[0]: {
            "row_count": row[1],
            "min_event_time": row[2],
            "max_event_time": row[3],
            "source_file_period": row[4],
        }
        for row in rows
    }
    for file_path, record in file_records.items():
        profile = profile_by_path.get(file_path, {})
        upsert_source_file_manifest(
            con,
            record,
            processed_status="PROCESSED",
            run_id=run_id,
            row_count=profile.get("row_count"),
            min_event_time=profile.get("min_event_time"),
            max_event_time=profile.get("max_event_time"),
            message=f"source_file_period={profile.get('source_file_period')}",
        )


def _update_historical_watermark(con, run_id: str):
    row = con.execute(
        """
        WITH latest_file AS (
            SELECT *
            FROM raw.historical_trip_file_profile
            QUALIFY row_number() OVER (
                ORDER BY max_started_at DESC NULLS LAST, source_file_name DESC
            ) = 1
        )
        SELECT
            (SELECT max(try_cast(started_at AS TIMESTAMP)) FROM raw.historical_trips) AS latest_trip_started_at,
            (SELECT max(try_cast(ended_at AS TIMESTAMP)) FROM raw.historical_trips) AS latest_trip_ended_at,
            source_file_period,
            source_file_name,
            source_file_path
        FROM latest_file
        """
    ).fetchone()
    if row:
        upsert_pipeline_watermark(
            con,
            source_name="historical_trips",
            source_type="historical_csv",
            run_id=run_id,
            latest_trip_started_at=row[0],
            latest_trip_ended_at=row[1],
            latest_source_file_period=row[2],
            latest_processed_file=row[3],
            latest_source_file_path=row[4],
        )


def ingest_historical_trips(input_dir: Path = DATA_DIR, full_refresh: bool = False, run_id: str | None = None):
    logger = configure_logging()
    files = _prepare_historical_trip_files(input_dir)
    if not files:
        raise FileNotFoundError(f"No Divvy trip CSV files found in {input_dir}")

    logger.info("Found %s historical trip files", len(files))
    con = get_connection()
    run_id = run_id or make_run_id()

    manifest_empty = (
        con.execute(
            """
            SELECT COUNT(*)
            FROM ops.source_file_manifest
            WHERE source_name = 'historical_trips'
            """
        ).fetchone()[0]
        == 0
    )
    force_full_refresh = full_refresh or not table_exists(con, "raw.historical_trips") or manifest_empty
    _ensure_historical_tables(con, full_refresh=force_full_refresh)

    existing_manifest = get_existing_manifest_records(con, files)
    records_by_path = {
        str(path): source_file_record("historical_trips", path)
        for path in files
    }
    files_to_process = [
        path
        for path in files
        if force_full_refresh
        or source_file_has_changed(existing_manifest.get(str(path)), records_by_path[str(path)])
    ]
    skipped_files = [path for path in files if path not in files_to_process]

    if files_to_process:
        logger.info("Processing %s new/changed historical trip files", len(files_to_process))
        _delete_source_paths(con, files_to_process)
        file_list_sql = _duckdb_file_list(files_to_process)
        con.execute(
            f"""
            INSERT INTO raw.historical_trips BY NAME
            SELECT
                regexp_extract(filename, '[^/]+$') AS source_file_name,
                filename AS source_file_path,
                regexp_extract(filename, '([0-9]{{6}})', 1) AS source_file_period,
                current_timestamp AS loaded_at_utc,
                ride_id,
                rideable_type,
                started_at,
                ended_at,
                start_station_name,
                start_station_id,
                end_station_name,
                end_station_id,
                start_lat,
                start_lng,
                end_lat,
                end_lng,
                member_casual
            FROM read_csv(
                {file_list_sql},
                header = true,
                union_by_name = true,
                filename = true,
                all_varchar = true
            )
            """
        )
        _insert_historical_profiles(con, files_to_process)
        _update_historical_manifest(
            con,
            run_id,
            {str(path): records_by_path[str(path)] for path in files_to_process},
        )
    else:
        logger.info("No new or changed historical trip files to ingest")

    for path in skipped_files:
        mark_source_file_skipped(
            con,
            records_by_path[str(path)],
            existing_manifest[str(path)],
            run_id,
        )

    _update_historical_watermark(con, run_id)

    row_count = count_table(con, "raw.historical_trips")
    logger.info("Loaded raw.historical_trips rows=%s", row_count)
    con.close()
    return {
        "rows_affected": row_count,
        "message": f"Processed {len(files_to_process)} and skipped {len(skipped_files)} of {len(files)} historical files",
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest historical Divvy trip CSV files into DuckDB raw tables.")
    parser.add_argument("--input-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    logger = configure_logging()
    run_id = make_run_id()
    con = get_connection()
    started_at = utc_now_naive()
    try:
        result = ingest_historical_trips(args.input_dir, full_refresh=args.full_refresh, run_id=run_id)
        record_pipeline_event(
            con,
            run_id,
            "ingest_historical_trips",
            "raw",
            str(args.input_dir),
            "raw.historical_trips",
            "SUCCESS",
            rows_affected=result["rows_affected"],
            started_at_utc=started_at,
            ended_at_utc=utc_now_naive(),
            message=result["message"],
        )
    except Exception as exc:
        logger.exception("Historical trip ingestion failed")
        record_pipeline_event(
            con,
            run_id,
            "ingest_historical_trips",
            "raw",
            str(args.input_dir),
            "raw.historical_trips",
            "FAILED",
            started_at_utc=started_at,
            ended_at_utc=utc_now_naive(),
            error_message=str(exc),
        )
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
