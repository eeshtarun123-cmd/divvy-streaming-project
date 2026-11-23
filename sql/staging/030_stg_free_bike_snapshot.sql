CREATE OR REPLACE TABLE staging.stg_free_bike_snapshot AS
SELECT
    bike_id,
    ingested_at_utc AS snapshot_at_utc,
    lat,
    lon,
    vehicle_type_id,
    current_range_meters,
    is_reserved,
    is_disabled,
    rental_uri_android,
    rental_uri_ios,
    source_file_name,
    source_file_path,
    feed_last_updated_at_utc,
    ttl_seconds,
    gbfs_version,
    lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AS has_valid_coordinates,
    coalesce(is_reserved, false) = false AND coalesce(is_disabled, false) = false AS free_bike_available_flag
FROM raw.gbfs_free_bike_status;
