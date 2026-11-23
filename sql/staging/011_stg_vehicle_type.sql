CREATE OR REPLACE TABLE staging.stg_vehicle_type AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY vehicle_type_id
            ORDER BY ingested_at_utc DESC, source_file_name DESC
        ) AS vehicle_type_row_number
    FROM raw.gbfs_vehicle_types
)
SELECT
    vehicle_type_id,
    form_factor,
    propulsion_type,
    vehicle_type_name,
    max_range_meters,
    source_file_name,
    source_file_path,
    ingested_at_utc,
    feed_last_updated_at_utc,
    ttl_seconds,
    gbfs_version
FROM ranked
WHERE vehicle_type_row_number = 1;
