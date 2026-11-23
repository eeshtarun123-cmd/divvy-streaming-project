CREATE OR REPLACE TABLE staging.stg_system_alerts AS
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
    ingested_at_utc AS snapshot_at_utc,
    source_file_name,
    source_file_path,
    feed_last_updated_at_utc,
    ttl_seconds,
    gbfs_version,
    (start_at_utc IS NULL OR start_at_utc <= ingested_at_utc)
        AND (end_at_utc IS NULL OR end_at_utc >= ingested_at_utc) AS alert_active_flag
FROM raw.gbfs_system_alerts;
