import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from pipeline_common import (
    RAW_DIR,
    configure_logging,
    count_table,
    get_existing_manifest_records,
    get_connection,
    mark_source_file_skipped,
    make_run_id,
    parse_utc_datetime,
    record_pipeline_event,
    source_file_has_changed,
    source_file_record,
    table_exists,
    upsert_pipeline_watermark,
    upsert_source_file_manifest,
    utc_now_naive,
)


GBFS_FEEDS = {
    "gbfs_feed_index": "https://gbfs.lyft.com/gbfs/2.3/chi/gbfs.json",
    "station_information": "https://gbfs.lyft.com/gbfs/2.3/chi/en/station_information.json",
    "station_status": "https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json",
    "free_bike_status": "https://gbfs.lyft.com/gbfs/2.3/chi/en/free_bike_status.json",
    "system_alerts": "https://gbfs.lyft.com/gbfs/2.3/chi/en/system_alerts.json",
    "vehicle_types": "https://gbfs.lyft.com/gbfs/2.3/chi/en/vehicle_types.json",
}

COMMON_COLS = """
    source_feed VARCHAR,
    source_url VARCHAR,
    source_file_path VARCHAR,
    source_file_name VARCHAR,
    ingested_at_utc TIMESTAMP,
    feed_last_updated_epoch BIGINT,
    feed_last_updated_at_utc TIMESTAMP,
    ttl_seconds BIGINT,
    gbfs_version VARCHAR,
    loaded_at_utc TIMESTAMP
"""

GBFS_TABLE_SCHEMAS = {
    "raw.gbfs_snapshot_files": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_snapshot_files (
            {COMMON_COLS},
            raw_response_json VARCHAR
        )
    """,
    "raw.gbfs_feed_index": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_feed_index (
            {COMMON_COLS},
            language VARCHAR,
            feed_name VARCHAR,
            feed_url VARCHAR
        )
    """,
    "raw.gbfs_station_information": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_station_information (
            {COMMON_COLS},
            station_id VARCHAR,
            name VARCHAR,
            short_name VARCHAR,
            address VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            capacity BIGINT,
            region_id VARCHAR,
            rental_uri_android VARCHAR,
            rental_uri_ios VARCHAR
        )
    """,
    "raw.gbfs_station_status": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_station_status (
            {COMMON_COLS},
            station_id VARCHAR,
            num_bikes_available BIGINT,
            num_bikes_disabled BIGINT,
            num_docks_available BIGINT,
            num_docks_disabled BIGINT,
            num_ebikes_available BIGINT,
            num_scooters_available BIGINT,
            num_scooters_unavailable BIGINT,
            is_installed BOOLEAN,
            is_renting BOOLEAN,
            is_returning BOOLEAN,
            last_reported_epoch BIGINT,
            last_reported_at_utc TIMESTAMP
        )
    """,
    "raw.gbfs_station_vehicle_types_available": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_station_vehicle_types_available (
            {COMMON_COLS},
            station_id VARCHAR,
            vehicle_type_id VARCHAR,
            available_count BIGINT
        )
    """,
    "raw.gbfs_free_bike_status": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_free_bike_status (
            {COMMON_COLS},
            bike_id VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            is_reserved BOOLEAN,
            is_disabled BOOLEAN,
            vehicle_type_id VARCHAR,
            current_range_meters DOUBLE,
            rental_uri_android VARCHAR,
            rental_uri_ios VARCHAR
        )
    """,
    "raw.gbfs_vehicle_types": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_vehicle_types (
            {COMMON_COLS},
            vehicle_type_id VARCHAR,
            form_factor VARCHAR,
            propulsion_type VARCHAR,
            vehicle_type_name VARCHAR,
            max_range_meters DOUBLE
        )
    """,
    "raw.gbfs_system_alerts": f"""
        CREATE TABLE IF NOT EXISTS raw.gbfs_system_alerts (
            {COMMON_COLS},
            alert_id VARCHAR,
            alert_type VARCHAR,
            summary VARCHAR,
            description VARCHAR,
            url VARCHAR,
            station_ids_json VARCHAR,
            region_ids_json VARCHAR,
            start_epoch BIGINT,
            end_epoch BIGINT,
            start_at_utc TIMESTAMP,
            end_at_utc TIMESTAMP
        )
    """,
}


def _epoch_to_utc(epoch_value):
    if epoch_value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(epoch_value), timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def _json_text(value):
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def _text_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("text") or value.get("value") or json.dumps(value, sort_keys=True)
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("language") == "en":
                return item.get("text") or item.get("value")
        for item in value:
            if isinstance(item, dict):
                text = item.get("text") or item.get("value")
                if text:
                    return text
        return json.dumps(value, sort_keys=True)
    return str(value)


def _first_alert_time(alert, key):
    times = alert.get("times")
    if isinstance(times, list) and times:
        return times[0].get(key)
    if isinstance(times, dict):
        return times.get(key)
    return alert.get(key)


def _base_meta(path: Path, payload: dict):
    raw_response = payload.get("raw_response", {})
    last_updated_epoch = raw_response.get("last_updated")
    return {
        "source_feed": payload.get("source_feed"),
        "source_url": payload.get("source_url"),
        "source_file_path": str(path),
        "source_file_name": path.name,
        "ingested_at_utc": parse_utc_datetime(payload.get("ingested_at_utc")),
        "feed_last_updated_epoch": last_updated_epoch,
        "feed_last_updated_at_utc": _epoch_to_utc(last_updated_epoch),
        "ttl_seconds": raw_response.get("ttl"),
        "gbfs_version": raw_response.get("version"),
        "loaded_at_utc": utc_now_naive(),
    }


def _ensure_gbfs_tables(con, full_refresh: bool = False):
    if full_refresh:
        for table_name in GBFS_TABLE_SCHEMAS:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute("DELETE FROM ops.source_file_manifest WHERE source_name LIKE 'gbfs.%'")

    for schema_sql in GBFS_TABLE_SCHEMAS.values():
        con.execute(schema_sql)


def _delete_source_paths_from_tables(con, paths):
    for path in paths:
        for table_name in GBFS_TABLE_SCHEMAS:
            con.execute(f"DELETE FROM {table_name} WHERE source_file_path = ?", [str(path)])


def _insert_rows(con, table_name, rows):
    if rows:
        df = pd.DataFrame(rows)
        con.register("_rows_to_insert", df)
        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM _rows_to_insert")
        con.unregister("_rows_to_insert")


def _update_gbfs_manifest(con, run_id: str, records_by_path: dict, file_metrics: dict):
    for file_path, record in records_by_path.items():
        metrics = file_metrics.get(file_path, {})
        upsert_source_file_manifest(
            con,
            record,
            processed_status="PROCESSED",
            run_id=run_id,
            row_count=metrics.get("row_count"),
            min_event_time=metrics.get("min_event_time"),
            max_event_time=metrics.get("max_event_time"),
            message=metrics.get("message"),
        )


def _update_gbfs_watermarks(con, run_id: str):
    rows = con.execute(
        """
        WITH ranked AS (
            SELECT
                source_feed,
                source_file_name,
                source_file_path,
                ingested_at_utc,
                feed_last_updated_at_utc,
                row_number() OVER (
                    PARTITION BY source_feed
                    ORDER BY ingested_at_utc DESC NULLS LAST, source_file_name DESC
                ) AS feed_row_number
            FROM raw.gbfs_snapshot_files
        )
        SELECT
            source_feed,
            source_file_name,
            source_file_path,
            ingested_at_utc,
            feed_last_updated_at_utc
        FROM ranked
        WHERE feed_row_number = 1
        """
    ).fetchall()

    for source_feed, source_file_name, source_file_path, ingested_at_utc, feed_last_updated_at_utc in rows:
        upsert_pipeline_watermark(
            con,
            source_name=f"gbfs.{source_feed}",
            source_type="gbfs_json",
            run_id=run_id,
            latest_ingested_at_utc=ingested_at_utc,
            latest_feed_last_updated_at_utc=feed_last_updated_at_utc,
            latest_processed_file=source_file_name,
            latest_source_file_path=source_file_path,
        )


def fetch_gbfs_snapshots(raw_dir: Path = RAW_DIR, feed_names=None):
    logger = configure_logging()
    raw_dir.mkdir(parents=True, exist_ok=True)
    snapshot_time = datetime.now(timezone.utc)
    snapshot_text = snapshot_time.strftime("%Y%m%d_%H%M%S")

    written_paths = []
    for feed_name in GBFS_FEEDS if feed_names is None else feed_names:
        feed_url = GBFS_FEEDS[feed_name]
        logger.info("Fetching GBFS feed=%s url=%s", feed_name, feed_url)
        response = requests.get(feed_url, timeout=20)
        response.raise_for_status()
        data = response.json()

        output_folder = raw_dir / feed_name
        output_folder.mkdir(parents=True, exist_ok=True)
        output_path = output_folder / f"{feed_name}_{snapshot_text}.json"
        payload = {
            "ingested_at_utc": snapshot_time.isoformat(),
            "source_feed": feed_name,
            "source_url": feed_url,
            "raw_response": data,
        }
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        written_paths.append(output_path)
        logger.info("Wrote GBFS raw snapshot=%s", output_path)

    return written_paths


def ingest_gbfs_raw_files(raw_dir: Path = RAW_DIR, full_refresh: bool = False, run_id: str | None = None):
    logger = configure_logging()
    files = sorted(path for path in raw_dir.glob("*/*.json") if path.is_file())
    if not files:
        raise FileNotFoundError(f"No GBFS raw JSON files found under {raw_dir}")

    con = get_connection()
    run_id = run_id or make_run_id()
    manifest_empty = (
        con.execute(
            """
            SELECT COUNT(*)
            FROM ops.source_file_manifest
            WHERE source_name LIKE 'gbfs.%'
            """
        ).fetchone()[0]
        == 0
    )
    all_tables_exist = all(table_exists(con, table_name) for table_name in GBFS_TABLE_SCHEMAS)
    force_full_refresh = full_refresh or manifest_empty or not all_tables_exist
    _ensure_gbfs_tables(con, full_refresh=force_full_refresh)

    existing_manifest = get_existing_manifest_records(con, files)
    records_by_path = {
        str(path): source_file_record(f"gbfs.{path.parent.name}", path)
        for path in files
    }
    files_to_process = [
        path
        for path in files
        if force_full_refresh
        or source_file_has_changed(existing_manifest.get(str(path)), records_by_path[str(path)])
    ]
    skipped_files = [path for path in files if path not in files_to_process]

    if not files_to_process:
        logger.info("No new or changed GBFS raw JSON files to ingest")
        for path in skipped_files:
            mark_source_file_skipped(
                con,
                records_by_path[str(path)],
                existing_manifest[str(path)],
                run_id,
            )
        _update_gbfs_watermarks(con, run_id)
        total_rows = sum(count_table(con, table) for table in GBFS_TABLE_SCHEMAS)
        con.close()
        return {
            "rows_affected": total_rows,
            "message": f"Processed 0 and skipped {len(skipped_files)} of {len(files)} GBFS JSON files",
        }

    snapshot_files = []
    feed_index_rows = []
    station_information_rows = []
    station_status_rows = []
    station_vehicle_type_rows = []
    free_bike_rows = []
    vehicle_type_rows = []
    alert_rows = []
    file_metrics = {}

    for path in files_to_process:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_response = payload.get("raw_response", {})
        data = raw_response.get("data", {})
        meta = _base_meta(path, payload)
        feed_name = payload.get("source_feed") or path.parent.name
        row_count = 0
        snapshot_files.append({**meta, "raw_response_json": json.dumps(raw_response)})

        if feed_name == "gbfs_feed_index":
            for language, language_payload in data.items():
                feeds = language_payload.get("feeds", []) if isinstance(language_payload, dict) else language_payload
                row_count += len(feeds or [])
                for feed in feeds or []:
                    feed_index_rows.append(
                        {
                            **meta,
                            "language": language,
                            "feed_name": feed.get("name"),
                            "feed_url": feed.get("url"),
                        }
                    )

        elif feed_name == "station_information":
            stations = data.get("stations", [])
            row_count = len(stations)
            for station in stations:
                rental_uris = station.get("rental_uris") or {}
                station_information_rows.append(
                    {
                        **meta,
                        "station_id": station.get("station_id"),
                        "name": station.get("name"),
                        "short_name": station.get("short_name"),
                        "address": station.get("address"),
                        "lat": station.get("lat"),
                        "lon": station.get("lon"),
                        "capacity": station.get("capacity"),
                        "region_id": station.get("region_id"),
                        "rental_uri_android": rental_uris.get("android"),
                        "rental_uri_ios": rental_uris.get("ios"),
                    }
                )

        elif feed_name == "station_status":
            stations = data.get("stations", [])
            row_count = len(stations)
            for station in stations:
                station_status_rows.append(
                    {
                        **meta,
                        "station_id": station.get("station_id"),
                        "num_bikes_available": station.get("num_bikes_available"),
                        "num_bikes_disabled": station.get("num_bikes_disabled"),
                        "num_docks_available": station.get("num_docks_available"),
                        "num_docks_disabled": station.get("num_docks_disabled"),
                        "num_ebikes_available": station.get("num_ebikes_available"),
                        "num_scooters_available": station.get("num_scooters_available"),
                        "num_scooters_unavailable": station.get("num_scooters_unavailable"),
                        "is_installed": station.get("is_installed"),
                        "is_renting": station.get("is_renting"),
                        "is_returning": station.get("is_returning"),
                        "last_reported_epoch": station.get("last_reported"),
                        "last_reported_at_utc": _epoch_to_utc(station.get("last_reported")),
                    }
                )
                for vehicle_type in station.get("vehicle_types_available") or []:
                    station_vehicle_type_rows.append(
                        {
                            **meta,
                            "station_id": station.get("station_id"),
                            "vehicle_type_id": vehicle_type.get("vehicle_type_id"),
                            "available_count": vehicle_type.get("count"),
                        }
                    )

        elif feed_name == "free_bike_status":
            bikes = data.get("bikes", [])
            row_count = len(bikes)
            for bike in bikes:
                rental_uris = bike.get("rental_uris") or {}
                free_bike_rows.append(
                    {
                        **meta,
                        "bike_id": bike.get("bike_id"),
                        "lat": bike.get("lat"),
                        "lon": bike.get("lon"),
                        "is_reserved": bike.get("is_reserved"),
                        "is_disabled": bike.get("is_disabled"),
                        "vehicle_type_id": bike.get("vehicle_type_id"),
                        "current_range_meters": bike.get("current_range_meters"),
                        "rental_uri_android": rental_uris.get("android"),
                        "rental_uri_ios": rental_uris.get("ios"),
                    }
                )

        elif feed_name == "vehicle_types":
            vehicle_types = data.get("vehicle_types", [])
            row_count = len(vehicle_types)
            for vehicle_type in vehicle_types:
                vehicle_type_rows.append(
                    {
                        **meta,
                        "vehicle_type_id": vehicle_type.get("vehicle_type_id"),
                        "form_factor": vehicle_type.get("form_factor"),
                        "propulsion_type": vehicle_type.get("propulsion_type"),
                        "vehicle_type_name": vehicle_type.get("name"),
                        "max_range_meters": vehicle_type.get("max_range_meters"),
                    }
                )

        elif feed_name == "system_alerts":
            alerts = data.get("alerts", [])
            row_count = len(alerts)
            for alert in alerts:
                start_epoch = _first_alert_time(alert, "start")
                end_epoch = _first_alert_time(alert, "end")
                alert_rows.append(
                    {
                        **meta,
                        "alert_id": alert.get("alert_id"),
                        "alert_type": alert.get("type"),
                        "summary": _text_value(alert.get("summary")),
                        "description": _text_value(alert.get("description")),
                        "url": _text_value(alert.get("url")),
                        "station_ids_json": _json_text(alert.get("station_ids")),
                        "region_ids_json": _json_text(alert.get("region_ids")),
                        "start_epoch": start_epoch,
                        "end_epoch": end_epoch,
                        "start_at_utc": _epoch_to_utc(start_epoch),
                        "end_at_utc": _epoch_to_utc(end_epoch),
                    }
                )

        file_metrics[str(path)] = {
            "row_count": row_count,
            "min_event_time": meta["ingested_at_utc"],
            "max_event_time": meta["feed_last_updated_at_utc"] or meta["ingested_at_utc"],
            "message": f"source_feed={feed_name}",
        }

    _delete_source_paths_from_tables(con, files_to_process)
    _insert_rows(con, "raw.gbfs_snapshot_files", snapshot_files)
    _insert_rows(con, "raw.gbfs_feed_index", feed_index_rows)
    _insert_rows(con, "raw.gbfs_station_information", station_information_rows)
    _insert_rows(con, "raw.gbfs_station_status", station_status_rows)
    _insert_rows(con, "raw.gbfs_station_vehicle_types_available", station_vehicle_type_rows)
    _insert_rows(con, "raw.gbfs_free_bike_status", free_bike_rows)
    _insert_rows(con, "raw.gbfs_vehicle_types", vehicle_type_rows)
    _insert_rows(con, "raw.gbfs_system_alerts", alert_rows)

    _update_gbfs_manifest(
        con,
        run_id,
        {str(path): records_by_path[str(path)] for path in files_to_process},
        file_metrics,
    )
    for path in skipped_files:
        mark_source_file_skipped(
            con,
            records_by_path[str(path)],
            existing_manifest[str(path)],
            run_id,
        )
    _update_gbfs_watermarks(con, run_id)

    total_rows = sum(
        count_table(con, table)
        for table in GBFS_TABLE_SCHEMAS
    )
    logger.info("Processed GBFS raw files=%s raw_rows=%s", len(files_to_process), total_rows)
    con.close()
    return {
        "rows_affected": total_rows,
        "message": f"Processed {len(files_to_process)} and skipped {len(skipped_files)} of {len(files)} GBFS JSON files",
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch and/or ingest Divvy GBFS live snapshots.")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--fetch", action="store_true", help="Fetch fresh GBFS snapshots before ingesting raw JSON files.")
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    logger = configure_logging()
    run_id = make_run_id()
    con = get_connection()
    started_at = utc_now_naive()
    try:
        if args.fetch:
            fetch_gbfs_snapshots(args.raw_dir)
        result = ingest_gbfs_raw_files(args.raw_dir, full_refresh=args.full_refresh, run_id=run_id)
        record_pipeline_event(
            con,
            run_id,
            "ingest_gbfs_live",
            "raw",
            str(args.raw_dir),
            "raw.gbfs_*",
            "SUCCESS",
            rows_affected=result["rows_affected"],
            started_at_utc=started_at,
            ended_at_utc=utc_now_naive(),
            message=result["message"],
        )
    except Exception as exc:
        logger.exception("GBFS ingestion failed")
        record_pipeline_event(
            con,
            run_id,
            "ingest_gbfs_live",
            "raw",
            str(args.raw_dir),
            "raw.gbfs_*",
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
