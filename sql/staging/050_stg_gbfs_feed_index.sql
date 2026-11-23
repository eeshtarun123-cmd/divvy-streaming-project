CREATE OR REPLACE TABLE staging.stg_gbfs_feed_index AS
SELECT
    language,
    feed_name,
    feed_url,
    ingested_at_utc AS snapshot_at_utc,
    source_file_name,
    source_file_path,
    feed_last_updated_at_utc,
    ttl_seconds,
    gbfs_version
FROM raw.gbfs_feed_index;
