CREATE OR REPLACE TABLE staging.stg_station AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY station_id
            ORDER BY ingested_at_utc DESC, source_file_name DESC
        ) AS station_row_number
    FROM raw.gbfs_station_information
)
SELECT
    station_id,
    name AS station_name,
    short_name,
    address,
    lat,
    lon,
    capacity::BIGINT AS station_capacity,
    region_id,
    rental_uri_android,
    rental_uri_ios,
    source_file_name,
    source_file_path,
    ingested_at_utc,
    feed_last_updated_at_utc,
    ttl_seconds,
    gbfs_version,
    station_capacity IS NOT NULL AND station_capacity > 0 AS has_valid_capacity,
    lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AS has_valid_coordinates
FROM ranked
WHERE station_row_number = 1;
