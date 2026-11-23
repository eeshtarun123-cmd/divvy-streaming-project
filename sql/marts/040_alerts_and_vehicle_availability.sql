CREATE OR REPLACE TABLE mart.mart_active_alerts AS
SELECT
    alert_id,
    alert_type,
    summary,
    description,
    url,
    station_ids_json,
    region_ids_json,
    start_at_utc,
    end_at_utc,
    snapshot_at_utc
FROM mart.fact_system_alerts
WHERE alert_active_flag
  AND snapshot_at_utc = (
      SELECT max(ingested_at_utc)
      FROM raw.gbfs_snapshot_files
      WHERE source_feed = 'system_alerts'
  );

CREATE OR REPLACE TABLE mart.mart_vehicle_availability AS
WITH latest_station_snapshot AS (
    SELECT max(snapshot_at_utc) AS snapshot_at_utc
    FROM staging.stg_station_vehicle_type_status_snapshot
),
station_vehicle AS (
    SELECT
        vehicle_type_id,
        SUM(available_count) AS station_available_count
    FROM staging.stg_station_vehicle_type_status_snapshot AS snapshot
    INNER JOIN latest_station_snapshot
        ON snapshot.snapshot_at_utc = latest_station_snapshot.snapshot_at_utc
    GROUP BY 1
),
latest_free_snapshot AS (
    SELECT max(snapshot_at_utc) AS snapshot_at_utc
    FROM mart.fact_free_bike_snapshot
),
free_vehicle AS (
    SELECT
        vehicle_type_id,
        COUNT(*) AS free_floating_count,
        COUNT_IF(free_bike_available_flag) AS free_floating_available_count,
        COUNT_IF(is_disabled) AS free_floating_disabled_count,
        COUNT_IF(is_reserved) AS free_floating_reserved_count
    FROM mart.fact_free_bike_snapshot AS snapshot
    INNER JOIN latest_free_snapshot
        ON snapshot.snapshot_at_utc = latest_free_snapshot.snapshot_at_utc
    GROUP BY 1
)
SELECT
    coalesce(station_vehicle.vehicle_type_id, free_vehicle.vehicle_type_id) AS vehicle_type_id,
    vehicle_type.form_factor,
    vehicle_type.propulsion_type,
    vehicle_type.vehicle_type_name,
    coalesce(station_vehicle.station_available_count, 0) AS station_available_count,
    coalesce(free_vehicle.free_floating_count, 0) AS free_floating_count,
    coalesce(free_vehicle.free_floating_available_count, 0) AS free_floating_available_count,
    coalesce(free_vehicle.free_floating_disabled_count, 0) AS free_floating_disabled_count,
    coalesce(free_vehicle.free_floating_reserved_count, 0) AS free_floating_reserved_count,
    coalesce(station_vehicle.station_available_count, 0)
        + coalesce(free_vehicle.free_floating_available_count, 0) AS total_available_count
FROM station_vehicle
FULL OUTER JOIN free_vehicle
    ON station_vehicle.vehicle_type_id = free_vehicle.vehicle_type_id
LEFT JOIN mart.dim_vehicle_type AS vehicle_type
    ON coalesce(station_vehicle.vehicle_type_id, free_vehicle.vehicle_type_id) = vehicle_type.vehicle_type_id;
