CREATE OR REPLACE TABLE mart.mart_station_pressure AS
WITH latest_snapshot AS (
    SELECT max(snapshot_at_utc) AS snapshot_at_utc
    FROM mart.fact_station_status_snapshot
),
latest_status AS (
    SELECT status.*
    FROM mart.fact_station_status_snapshot AS status
    INNER JOIN latest_snapshot
        ON status.snapshot_at_utc = latest_snapshot.snapshot_at_utc
)
SELECT
    status.snapshot_at_utc,
    status.station_id,
    status.station_name,
    status.lat,
    status.lon,
    status.region_id,
    status.station_capacity,
    status.available_bikes,
    status.available_ebikes,
    status.disabled_bikes,
    status.available_docks,
    status.disabled_docks,
    status.available_scooters,
    status.unavailable_scooters,
    status.station_active_flag,
    status.bike_availability_rate,
    status.dock_availability_rate,
    status.station_pressure_score,
    status.empty_pressure_score,
    status.full_pressure_score,
    status.overall_pressure_score,
    status.empty_risk_flag,
    status.full_risk_flag,
    CASE
        WHEN status.station_active_flag = false THEN 'inactive'
        WHEN status.empty_risk_flag THEN 'empty_risk'
        WHEN status.full_risk_flag THEN 'full_risk'
        WHEN status.bike_availability_rate <= 0.20 THEN 'low_bikes'
        WHEN status.dock_availability_rate <= 0.20 THEN 'low_docks'
        ELSE 'balanced'
    END AS station_pressure_category,
    status.capacity_missing_or_zero_flag,
    status.has_station_dimension_match
FROM latest_status AS status;
