CREATE OR REPLACE TABLE mart.mart_system_overview AS
WITH station_pressure AS (
    SELECT
        snapshot_at_utc,
        COUNT(*) AS station_count,
        COUNT_IF(station_active_flag) AS active_station_count,
        COUNT_IF(empty_risk_flag) AS empty_risk_station_count,
        COUNT_IF(full_risk_flag) AS full_risk_station_count,
        SUM(available_bikes) AS available_bikes,
        SUM(available_ebikes) AS available_ebikes,
        SUM(available_scooters) AS available_scooters,
        SUM(available_docks) AS available_docks
    FROM mart.mart_station_pressure
    GROUP BY 1
),
free_bikes AS (
    SELECT
        COUNT(*) AS free_bike_count,
        COUNT_IF(free_bike_available_flag) AS free_bike_available_count,
        COUNT_IF(is_disabled) AS free_bike_disabled_count,
        COUNT_IF(is_reserved) AS free_bike_reserved_count
    FROM mart.fact_free_bike_snapshot
    WHERE snapshot_at_utc = (SELECT max(snapshot_at_utc) FROM mart.fact_free_bike_snapshot)
),
alerts AS (
    SELECT COUNT(*) AS active_alert_count
    FROM mart.mart_active_alerts
),
trips AS (
    SELECT
        COUNT(*) AS historical_trip_count,
        MIN(started_date) AS historical_start_date,
        MAX(started_date) AS historical_end_date,
        COUNT(DISTINCT started_month) AS historical_month_count
    FROM mart.fact_trips
)
SELECT
    station_pressure.snapshot_at_utc,
    station_pressure.station_count,
    station_pressure.active_station_count,
    station_pressure.empty_risk_station_count,
    station_pressure.full_risk_station_count,
    station_pressure.available_bikes,
    station_pressure.available_ebikes,
    station_pressure.available_scooters,
    station_pressure.available_docks,
    free_bikes.free_bike_count,
    free_bikes.free_bike_available_count,
    free_bikes.free_bike_disabled_count,
    free_bikes.free_bike_reserved_count,
    alerts.active_alert_count,
    trips.historical_trip_count,
    trips.historical_start_date,
    trips.historical_end_date,
    trips.historical_month_count
FROM station_pressure
CROSS JOIN free_bikes
CROSS JOIN alerts
CROSS JOIN trips;
