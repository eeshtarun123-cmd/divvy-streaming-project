CREATE OR REPLACE TABLE staging.stg_station_vehicle_type_status_snapshot AS
SELECT
    station_id,
    ingested_at_utc AS snapshot_at_utc,
    vehicle_type_id,
    available_count,
    source_file_name,
    source_file_path,
    feed_last_updated_at_utc,
    ttl_seconds,
    gbfs_version
FROM raw.gbfs_station_vehicle_types_available;
