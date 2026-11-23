CREATE OR REPLACE TABLE staging.stg_station_status_snapshot AS
SELECT
    status.station_id,
    status.ingested_at_utc AS snapshot_at_utc,
    CAST(status.ingested_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS DATE) AS snapshot_date,
    CAST(date_part('hour', status.ingested_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') AS INTEGER) AS snapshot_hour,
    status.last_reported_at_utc AS status_last_reported_at_utc,
    status.num_bikes_available AS available_bikes,
    status.num_bikes_disabled AS disabled_bikes,
    status.num_docks_available AS available_docks,
    status.num_docks_disabled AS disabled_docks,
    status.num_ebikes_available AS available_ebikes,
    status.num_scooters_available AS available_scooters,
    status.num_scooters_unavailable AS unavailable_scooters,
    status.is_installed,
    status.is_renting,
    status.is_returning,
    station.station_name,
    station.lat,
    station.lon,
    station.station_capacity,
    station.region_id,
    status.source_file_name,
    status.source_file_path,
    status.feed_last_updated_at_utc,
    status.ttl_seconds,
    status.gbfs_version,
    coalesce(status.is_installed, false)
        AND coalesce(status.is_renting, false)
        AND coalesce(status.is_returning, false) AS station_active_flag,
    station.station_id IS NOT NULL AS has_station_dimension_match,
    station.station_capacity IS NULL OR station.station_capacity <= 0 AS capacity_missing_or_zero_flag,
    CASE
        WHEN station.station_capacity > 0 THEN status.num_bikes_available::DOUBLE / station.station_capacity
        ELSE NULL
    END AS bike_availability_rate,
    CASE
        WHEN station.station_capacity > 0 THEN status.num_docks_available::DOUBLE / station.station_capacity
        ELSE NULL
    END AS dock_availability_rate,
    CASE
        WHEN station.station_capacity > 0 THEN status.num_bikes_available::DOUBLE / station.station_capacity
        ELSE NULL
    END AS station_pressure_score,
    CASE
        WHEN station.station_capacity > 0 THEN greatest(1 - (status.num_bikes_available::DOUBLE / station.station_capacity), 0)
        ELSE NULL
    END AS empty_pressure_score,
    CASE
        WHEN station.station_capacity > 0 THEN greatest(1 - (status.num_docks_available::DOUBLE / station.station_capacity), 0)
        ELSE NULL
    END AS full_pressure_score,
    greatest(
        coalesce(
            CASE
                WHEN station.station_capacity > 0 THEN greatest(1 - (status.num_bikes_available::DOUBLE / station.station_capacity), 0)
                ELSE NULL
            END,
            0
        ),
        coalesce(
            CASE
                WHEN station.station_capacity > 0 THEN greatest(1 - (status.num_docks_available::DOUBLE / station.station_capacity), 0)
                ELSE NULL
            END,
            0
        )
    ) AS overall_pressure_score,
    coalesce(status.is_installed, false)
        AND coalesce(status.is_renting, false)
        AND coalesce(status.is_returning, false)
        AND status.num_bikes_available IS NOT NULL
        AND status.num_bikes_available <= 2 AS empty_risk_flag,
    coalesce(status.is_installed, false)
        AND coalesce(status.is_renting, false)
        AND coalesce(status.is_returning, false)
        AND status.num_docks_available IS NOT NULL
        AND status.num_docks_available <= 2 AS full_risk_flag
FROM raw.gbfs_station_status AS status
LEFT JOIN staging.stg_station AS station
    ON status.station_id = station.station_id;
