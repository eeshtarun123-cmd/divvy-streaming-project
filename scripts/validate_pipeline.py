import argparse
from dataclasses import dataclass

from pipeline_common import (
    configure_logging,
    get_connection,
    make_run_id,
    record_validation_result,
    table_exists,
)


class ValidationFailure(RuntimeError):
    pass


@dataclass
class SqlValidation:
    validation_name: str
    layer_name: str
    table_name: str
    severity: str
    sql: str
    threshold: int = 0
    message: str = ""


REQUIRED_COLUMNS = {
    "raw.historical_trips": [
        "source_file_name",
        "source_file_path",
        "source_file_period",
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_id",
        "end_station_id",
        "member_casual",
    ],
    "raw.historical_trip_file_profile": [
        "source_file_name",
        "source_file_path",
        "row_count",
        "min_started_at",
        "max_started_at",
    ],
    "staging.stg_station": [
        "station_id",
        "station_name",
        "lat",
        "lon",
        "station_capacity",
    ],
    "staging.stg_station_status_snapshot": [
        "station_id",
        "snapshot_at_utc",
        "available_bikes",
        "available_docks",
        "is_installed",
        "is_renting",
        "is_returning",
        "status_last_reported_at_utc",
    ],
    "staging.stg_trips": [
        "ride_id",
        "started_at_local",
        "ended_at_local",
        "trip_duration_minutes",
        "is_analytics_eligible",
    ],
    "mart.fact_trips": [
        "ride_id",
        "started_at_local",
        "ended_at_local",
        "trip_duration_minutes",
    ],
    "mart.mart_station_pressure": [
        "station_id",
        "station_capacity",
        "station_pressure_score",
        "empty_pressure_score",
        "full_pressure_score",
        "overall_pressure_score",
    ],
    "mart.mart_rebalancing_priority": [
        "station_id",
        "station_name",
        "risk_category",
        "rebalancing_priority_score",
        "is_ranking_eligible",
    ],
    "mart.dim_date": [
        "date_key",
        "date_day",
        "year_number",
        "month_number",
        "day_number",
        "day_of_week_name",
        "is_weekend",
    ],
    "mart.dim_time": [
        "hour_key",
        "hour_number",
        "hour_of_day",
        "hour_label",
        "time_period",
        "is_commute_period",
        "daypart",
        "is_commute_peak",
    ],
    "mart.mart_pipeline_health": [
        "run_id",
        "pipeline_started_at_utc",
        "pipeline_ended_at_utc",
        "total_duration_seconds",
        "total_steps",
        "successful_steps",
        "failed_steps",
        "total_validation_checks",
        "passed_validation_checks",
        "error_validation_failures",
        "warning_validation_failures",
        "total_manifest_files",
        "processed_files",
        "skipped_files",
        "total_watermarks",
        "pipeline_health_status",
    ],
}

OPTIONAL_TABLE_SEVERITY = {
    "mart.mart_pipeline_health": "WARNING",
}


RAW_CHECKS = [
    SqlValidation(
        "raw_station_information_min_rows",
        "raw",
        "raw.gbfs_station_information",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) > 1900 THEN 0 ELSE 1 END FROM raw.gbfs_station_information",
        message="raw.gbfs_station_information should have more than 1900 station rows.",
    ),
    SqlValidation(
        "raw_station_status_min_rows",
        "raw",
        "raw.gbfs_station_status",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) > 1900 THEN 0 ELSE 1 END FROM raw.gbfs_station_status",
        message="raw.gbfs_station_status should have more than 1900 station rows.",
    ),
    SqlValidation(
        "raw_vehicle_types_min_rows",
        "raw",
        "raw.gbfs_vehicle_types",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) >= 1 THEN 0 ELSE 1 END FROM raw.gbfs_vehicle_types",
        message="raw.gbfs_vehicle_types should have at least one row.",
    ),
    SqlValidation(
        "raw_feed_index_min_rows",
        "raw",
        "raw.gbfs_feed_index",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) >= 6 THEN 0 ELSE 1 END FROM raw.gbfs_feed_index",
        message="raw.gbfs_feed_index should have at least six rows.",
    ),
    SqlValidation(
        "raw_free_bike_status_present",
        "raw",
        "raw.gbfs_free_bike_status",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) >= 0 THEN 0 ELSE 1 END FROM raw.gbfs_free_bike_status",
        message="free_bike_status may be zero, but the table must be queryable.",
    ),
    SqlValidation(
        "raw_system_alerts_present",
        "raw",
        "raw.gbfs_system_alerts",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) >= 0 THEN 0 ELSE 1 END FROM raw.gbfs_system_alerts",
        message="system_alerts may be zero when no alerts are active.",
    ),
    SqlValidation(
        "raw_historical_trips_nonempty",
        "raw",
        "raw.historical_trips",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM raw.historical_trips",
        message="raw.historical_trips should not be empty.",
    ),
    SqlValidation(
        "raw_historical_profile_nonempty",
        "raw",
        "raw.historical_trip_file_profile",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM raw.historical_trip_file_profile",
        message="raw.historical_trip_file_profile should not be empty.",
    ),
    SqlValidation(
        "raw_trip_ride_id_not_null",
        "raw",
        "raw.historical_trips",
        "ERROR",
        "SELECT COUNT(*) FROM raw.historical_trips WHERE nullif(trim(ride_id), '') IS NULL",
        message="Historical trip ride_id must be present.",
    ),
    SqlValidation(
        "raw_trip_duplicate_ride_id",
        "raw",
        "raw.historical_trips",
        "WARNING",
        """
        SELECT COUNT(*)
        FROM (
            SELECT ride_id
            FROM raw.historical_trips
            WHERE nullif(trim(ride_id), '') IS NOT NULL
            GROUP BY ride_id
            HAVING COUNT(*) > 1
        )
        """,
        message="Duplicate raw ride_id values are retained in raw/staging and excluded from mart.fact_trips.",
    ),
    SqlValidation(
        "raw_trip_started_at_not_null",
        "raw",
        "raw.historical_trips",
        "ERROR",
        "SELECT COUNT(*) FROM raw.historical_trips WHERE try_cast(started_at AS TIMESTAMP) IS NULL",
        message="Historical started_at must parse as a timestamp.",
    ),
    SqlValidation(
        "raw_trip_ended_at_not_null",
        "raw",
        "raw.historical_trips",
        "ERROR",
        "SELECT COUNT(*) FROM raw.historical_trips WHERE try_cast(ended_at AS TIMESTAMP) IS NULL",
        message="Historical ended_at must parse as a timestamp.",
    ),
    SqlValidation(
        "raw_trip_ended_after_started",
        "raw",
        "raw.historical_trips",
        "WARNING",
        """
        SELECT COUNT(*)
        FROM raw.historical_trips
        WHERE try_cast(ended_at AS TIMESTAMP) <= try_cast(started_at AS TIMESTAMP)
        """,
        message="Non-positive trip durations are retained in raw/staging and excluded from mart.fact_trips.",
    ),
    SqlValidation(
        "raw_trip_short_duration",
        "raw",
        "raw.historical_trips",
        "WARNING",
        """
        SELECT COUNT(*)
        FROM raw.historical_trips
        WHERE date_diff('second', try_cast(started_at AS TIMESTAMP), try_cast(ended_at AS TIMESTAMP)) / 60.0 <= 1
        """,
        message="Trips at or below 1 minute are retained in raw/staging but excluded from fact_trips.",
    ),
    SqlValidation(
        "raw_trip_over_24h_duration",
        "raw",
        "raw.historical_trips",
        "WARNING",
        """
        SELECT COUNT(*)
        FROM raw.historical_trips
        WHERE date_diff('second', try_cast(started_at AS TIMESTAMP), try_cast(ended_at AS TIMESTAMP)) / 60.0 >= 1440
        """,
        message="Trips at or above 24 hours are retained in raw/staging but excluded from fact_trips.",
    ),
    SqlValidation(
        "raw_trip_member_casual_valid",
        "raw",
        "raw.historical_trips",
        "ERROR",
        """
        SELECT COUNT(*)
        FROM raw.historical_trips
        WHERE lower(nullif(trim(member_casual), '')) NOT IN ('member', 'casual')
           OR nullif(trim(member_casual), '') IS NULL
        """,
        message="member_casual must be member or casual.",
    ),
    SqlValidation(
        "raw_trip_rideable_type_valid",
        "raw",
        "raw.historical_trips",
        "ERROR",
        """
        SELECT COUNT(*)
        FROM raw.historical_trips
        WHERE lower(nullif(trim(rideable_type), '')) NOT IN ('classic_bike', 'electric_bike', 'docked_bike')
           OR nullif(trim(rideable_type), '') IS NULL
        """,
        message="rideable_type must be a known Divvy rideable type.",
    ),
]


STAGING_CHECKS = [
    SqlValidation(
        "stg_station_id_not_null",
        "staging",
        "staging.stg_station",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_station WHERE station_id IS NULL",
    ),
    SqlValidation(
        "stg_station_id_unique",
        "staging",
        "staging.stg_station",
        "ERROR",
        "SELECT COUNT(*) - COUNT(DISTINCT station_id) FROM staging.stg_station",
    ),
    SqlValidation(
        "stg_station_name_not_null",
        "staging",
        "staging.stg_station",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_station WHERE station_name IS NULL",
    ),
    SqlValidation(
        "stg_station_lat_lon_not_null",
        "staging",
        "staging.stg_station",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_station WHERE lat IS NULL OR lon IS NULL",
    ),
    SqlValidation(
        "stg_station_capacity_positive",
        "staging",
        "staging.stg_station",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_station WHERE station_capacity IS NULL OR station_capacity <= 0",
    ),
    SqlValidation(
        "stg_station_status_station_id_not_null",
        "staging",
        "staging.stg_station_status_snapshot",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_station_status_snapshot WHERE station_id IS NULL",
    ),
    SqlValidation(
        "stg_station_status_nonnegative_counts",
        "staging",
        "staging.stg_station_status_snapshot",
        "ERROR",
        """
        SELECT COUNT(*)
        FROM staging.stg_station_status_snapshot
        WHERE available_bikes < 0
           OR available_docks < 0
           OR disabled_bikes < 0
           OR disabled_docks < 0
        """,
    ),
    SqlValidation(
        "stg_station_status_flags_not_null",
        "staging",
        "staging.stg_station_status_snapshot",
        "ERROR",
        """
        SELECT COUNT(*)
        FROM staging.stg_station_status_snapshot
        WHERE is_installed IS NULL
           OR is_renting IS NULL
           OR is_returning IS NULL
        """,
    ),
    SqlValidation(
        "stg_station_status_last_reported_not_null",
        "staging",
        "staging.stg_station_status_snapshot",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_station_status_snapshot WHERE status_last_reported_at_utc IS NULL",
    ),
    SqlValidation(
        "stg_trips_ride_id_not_null",
        "staging",
        "staging.stg_trips",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_trips WHERE ride_id IS NULL",
    ),
    SqlValidation(
        "stg_trips_started_ended_not_null",
        "staging",
        "staging.stg_trips",
        "ERROR",
        "SELECT COUNT(*) FROM staging.stg_trips WHERE started_at_local IS NULL OR ended_at_local IS NULL",
    ),
    SqlValidation(
        "stg_trips_short_duration",
        "staging",
        "staging.stg_trips",
        "WARNING",
        "SELECT COUNT(*) FROM staging.stg_trips WHERE trip_duration_minutes <= 1",
        message="Short trips are retained in staging and excluded from mart.fact_trips.",
    ),
    SqlValidation(
        "stg_trips_over_24h_duration",
        "staging",
        "staging.stg_trips",
        "WARNING",
        "SELECT COUNT(*) FROM staging.stg_trips WHERE trip_duration_minutes >= 1440",
        message="Long trips are retained in staging and excluded from mart.fact_trips.",
    ),
]


MART_CHECKS = [
    SqlValidation(
        "mart_station_pressure_station_id_not_null",
        "mart",
        "mart.mart_station_pressure",
        "ERROR",
        "SELECT COUNT(*) FROM mart.mart_station_pressure WHERE station_id IS NULL",
    ),
    SqlValidation(
        "mart_station_pressure_capacity_positive",
        "mart",
        "mart.mart_station_pressure",
        "ERROR",
        "SELECT COUNT(*) FROM mart.mart_station_pressure WHERE station_capacity IS NULL OR station_capacity <= 0",
    ),
    SqlValidation(
        "mart_station_pressure_score_not_null",
        "mart",
        "mart.mart_station_pressure",
        "ERROR",
        "SELECT COUNT(*) FROM mart.mart_station_pressure WHERE station_pressure_score IS NULL",
    ),
    SqlValidation(
        "mart_station_pressure_score_reasonable",
        "mart",
        "mart.mart_station_pressure",
        "WARNING",
        "SELECT COUNT(*) FROM mart.mart_station_pressure WHERE station_pressure_score < 0 OR station_pressure_score > 1.5",
        message="Legacy station_pressure_score mirrors bike availability; values above 1.5 usually mean availability exceeds capacity metadata.",
    ),
    SqlValidation(
        "mart_overall_pressure_score_reasonable",
        "mart",
        "mart.mart_station_pressure",
        "ERROR",
        "SELECT COUNT(*) FROM mart.mart_station_pressure WHERE overall_pressure_score IS NULL OR overall_pressure_score < 0 OR overall_pressure_score > 1.5",
        message="Overall pressure should be present and bounded.",
    ),
    SqlValidation(
        "mart_rebalancing_required_fields",
        "mart",
        "mart.mart_rebalancing_priority",
        "ERROR",
        """
        SELECT COUNT(*)
        FROM mart.mart_rebalancing_priority
        WHERE station_id IS NULL
           OR station_name IS NULL
           OR is_ranking_eligible IS NULL
           OR (rebalancing_priority_score IS NOT NULL) IS DISTINCT FROM is_ranking_eligible
           OR (recommended_action = 'station_inactive') IS DISTINCT FROM (NOT is_ranking_eligible)
           OR risk_category IS NULL
        """,
    ),
    SqlValidation(
        "mart_fact_trips_ride_id_unique",
        "mart",
        "mart.fact_trips",
        "ERROR",
        "SELECT COUNT(*) - COUNT(DISTINCT ride_id) FROM mart.fact_trips",
    ),
    SqlValidation(
        "mart_fact_trips_required_fields",
        "mart",
        "mart.fact_trips",
        "ERROR",
        """
        SELECT COUNT(*)
        FROM mart.fact_trips
        WHERE ride_id IS NULL
           OR started_at_local IS NULL
           OR ended_at_local IS NULL
        """,
    ),
    SqlValidation(
        "mart_fact_trips_duration_gt_1",
        "mart",
        "mart.fact_trips",
        "ERROR",
        "SELECT COUNT(*) FROM mart.fact_trips WHERE trip_duration_minutes <= 1",
    ),
    SqlValidation(
        "mart_fact_trips_duration_lt_24h",
        "mart",
        "mart.fact_trips",
        "ERROR",
        "SELECT COUNT(*) FROM mart.fact_trips WHERE trip_duration_minutes >= 1440",
    ),
    SqlValidation(
        "mart_live_station_status_maps_to_station_dimension",
        "mart",
        "mart.mart_station_pressure",
        "ERROR",
        "SELECT COUNT(*) FROM mart.mart_station_pressure WHERE has_station_dimension_match = false",
    ),
    SqlValidation(
        "mart_historical_start_station_mapping_warning",
        "mart",
        "mart.mart_trip_station_mapping_quality",
        "WARNING",
        "SELECT start_station_unmapped_trip_count FROM mart.mart_trip_station_mapping_quality",
        message="Historical stations can be retired or renamed, so unmapped history is a warning.",
    ),
    SqlValidation(
        "mart_historical_end_station_mapping_warning",
        "mart",
        "mart.mart_trip_station_mapping_quality",
        "WARNING",
        "SELECT end_station_unmapped_trip_count FROM mart.mart_trip_station_mapping_quality",
        message="Historical stations can be retired or renamed, so unmapped history is a warning.",
    ),
    SqlValidation(
        "mart_dim_date_nonempty",
        "mart",
        "mart.dim_date",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM mart.dim_date",
        message="dim_date should cover the historical trip date range.",
    ),
    SqlValidation(
        "mart_dim_time_has_24_hours",
        "mart",
        "mart.dim_time",
        "ERROR",
        "SELECT CASE WHEN COUNT(*) = 24 THEN 0 ELSE 1 END FROM mart.dim_time",
        message="dim_time should contain one row for each hour of day.",
    ),
    SqlValidation(
        "mart_dim_time_hours_unique",
        "mart",
        "mart.dim_time",
        "ERROR",
        "SELECT COUNT(*) - COUNT(DISTINCT hour_of_day) FROM mart.dim_time",
        message="dim_time hour_of_day values should be unique.",
    ),
    SqlValidation(
        "mart_pipeline_health_single_row",
        "mart",
        "mart.mart_pipeline_health",
        "WARNING",
        "SELECT CASE WHEN COUNT(*) = 1 THEN 0 ELSE 1 END FROM mart.mart_pipeline_health",
        message="mart_pipeline_health should return one row for the latest pipeline run.",
    ),
]


CHECKS_BY_LAYER = {
    "raw": RAW_CHECKS,
    "staging": STAGING_CHECKS,
    "mart": MART_CHECKS,
}


def _status(severity: str, failed_count: int, threshold: int) -> str:
    if failed_count <= threshold:
        return "PASS"
    if severity == "ERROR":
        return "FAIL"
    return severity


def _table_columns(con, qualified_table: str):
    schema_name, table_name = qualified_table.split(".", 1)
    return {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
            """,
            [schema_name, table_name],
        ).fetchall()
    }


def _record_table_existence(con, run_id: str, qualified_table: str):
    layer_name = qualified_table.split(".", 1)[0]
    severity = OPTIONAL_TABLE_SEVERITY.get(qualified_table, "ERROR")
    exists = table_exists(con, qualified_table)
    failed_count = 0 if exists else 1
    status = _status(severity, failed_count, 0)
    record_validation_result(
        con,
        run_id,
        validation_name=f"{qualified_table}_exists",
        layer_name=layer_name,
        table_name=qualified_table,
        severity=severity,
        status=status,
        failed_count=failed_count,
        threshold=0,
        message=f"{qualified_table} must exist.",
    )
    return exists, status


def _record_required_columns(con, run_id: str, qualified_table: str):
    layer_name = qualified_table.split(".", 1)[0]
    if not table_exists(con, qualified_table):
        return "FAIL"
    severity = OPTIONAL_TABLE_SEVERITY.get(qualified_table, "ERROR")
    actual_columns = _table_columns(con, qualified_table)
    required_columns = REQUIRED_COLUMNS.get(qualified_table, [])
    missing = [column for column in required_columns if column not in actual_columns]
    failed_count = len(missing)
    status = _status(severity, failed_count, 0)
    record_validation_result(
        con,
        run_id,
        validation_name=f"{qualified_table}_required_columns",
        layer_name=layer_name,
        table_name=qualified_table,
        severity=severity,
        status=status,
        failed_count=failed_count,
        threshold=0,
        message="Missing columns: " + ", ".join(missing) if missing else "Required columns are present.",
    )
    return status


def _run_sql_check(con, run_id: str, check: SqlValidation):
    try:
        failed_count = con.execute(check.sql).fetchone()[0]
        failed_count = int(failed_count or 0)
        status = _status(check.severity, failed_count, check.threshold)
        message = check.message or check.validation_name
    except Exception as exc:
        failed_count = 1
        status = "FAIL" if check.severity == "ERROR" else check.severity
        message = f"Validation query failed: {exc}"

    record_validation_result(
        con,
        run_id,
        validation_name=check.validation_name,
        layer_name=check.layer_name,
        table_name=check.table_name,
        severity=check.severity,
        status=status,
        failed_count=failed_count,
        threshold=check.threshold,
        message=message,
    )
    return status


def validate_layer(layer_name: str, run_id: str | None = None, stop_on_error: bool = True):
    if layer_name not in CHECKS_BY_LAYER:
        raise ValueError(f"Unknown validation layer: {layer_name}")

    logger = configure_logging()
    run_id = run_id or make_run_id()
    con = get_connection()
    checks = CHECKS_BY_LAYER[layer_name]
    tables = sorted({check.table_name for check in checks} | {table for table in REQUIRED_COLUMNS if table.startswith(layer_name + ".")})

    statuses = []
    for table in tables:
        _, exists_status = _record_table_existence(con, run_id, table)
        statuses.append(exists_status)
        if exists_status == "PASS":
            statuses.append(_record_required_columns(con, run_id, table))

    for check in checks:
        if table_exists(con, check.table_name):
            statuses.append(_run_sql_check(con, run_id, check))

    failed_errors = con.execute(
        """
        SELECT COUNT(*)
        FROM ops.validation_results
        WHERE run_id = ?
          AND layer_name = ?
          AND severity = 'ERROR'
          AND status = 'FAIL'
        """,
        [run_id, layer_name],
    ).fetchone()[0]
    warnings = con.execute(
        """
        SELECT COUNT(*)
        FROM ops.validation_results
        WHERE run_id = ?
          AND layer_name = ?
          AND status = 'WARNING'
        """,
        [run_id, layer_name],
    ).fetchone()[0]
    total = con.execute(
        """
        SELECT COUNT(*)
        FROM ops.validation_results
        WHERE run_id = ?
          AND layer_name = ?
        """,
        [run_id, layer_name],
    ).fetchone()[0]
    con.close()

    message = f"{layer_name} validations: {total} checks, {failed_errors} error failures, {warnings} warnings"
    logger.info(message)
    if failed_errors and stop_on_error:
        raise ValidationFailure(message)
    return {"rows_affected": total, "message": message}


def validate_all(run_id: str | None = None, stop_on_error: bool = True):
    run_id = run_id or make_run_id()
    total = 0
    messages = []
    for layer_name in ["raw", "staging", "mart"]:
        result = validate_layer(layer_name, run_id=run_id, stop_on_error=stop_on_error)
        total += result["rows_affected"]
        messages.append(result["message"])
    return {"rows_affected": total, "message": " | ".join(messages)}


def main():
    parser = argparse.ArgumentParser(description="Run Divvy pipeline validation checks.")
    parser.add_argument("--layer", choices=["raw", "staging", "mart", "all"], default="all")
    parser.add_argument("--no-stop-on-error", action="store_true")
    args = parser.parse_args()

    run_id = make_run_id()
    if args.layer == "all":
        validate_all(run_id=run_id, stop_on_error=not args.no_stop_on_error)
    else:
        validate_layer(args.layer, run_id=run_id, stop_on_error=not args.no_stop_on_error)


if __name__ == "__main__":
    main()
