CREATE OR REPLACE TABLE mart.fact_trips AS
SELECT
    trips.ride_id,
    trips.rideable_type,
    trips.started_at_local,
    trips.ended_at_local,
    trips.trip_duration_minutes,
    trips.started_date,
    trips.started_date AS trip_start_date,
    trips.started_month,
    trips.started_year,
    trips.started_month_number,
    trips.started_hour,
    trips.started_hour AS trip_start_hour,
    trips.started_day_of_week_number,
    trips.started_day_of_week_name,
    trips.start_station_id AS source_start_station_id,
    trips.start_station_name AS source_start_station_name,
    start_station_identifier.station_id AS start_gbfs_station_id,
    start_station.station_name AS start_gbfs_station_name,
    trips.end_station_id AS source_end_station_id,
    trips.end_station_name AS source_end_station_name,
    end_station_identifier.station_id AS end_gbfs_station_id,
    end_station.station_name AS end_gbfs_station_name,
    trips.start_lat,
    trips.start_lng,
    trips.end_lat,
    trips.end_lng,
    trips.member_casual,
    trips.source_file_name,
    trips.source_file_period,
    trips.source_file_month,
    trips.has_start_station_id,
    trips.has_end_station_id,
    start_station_identifier.station_id IS NOT NULL AS start_station_mapped_to_gbfs,
    end_station_identifier.station_id IS NOT NULL AS end_station_mapped_to_gbfs,
    trips.has_valid_start_coordinates,
    trips.has_valid_end_coordinates,
    trips.is_station_trip
FROM staging.stg_trips AS trips
LEFT JOIN mart.dim_station_identifier AS start_station_identifier
    ON trips.start_station_id = start_station_identifier.station_identifier
LEFT JOIN mart.dim_station AS start_station
    ON start_station_identifier.station_id = start_station.station_id
LEFT JOIN mart.dim_station_identifier AS end_station_identifier
    ON trips.end_station_id = end_station_identifier.station_identifier
LEFT JOIN mart.dim_station AS end_station
    ON end_station_identifier.station_id = end_station.station_id
WHERE trips.is_analytics_eligible;

CREATE OR REPLACE TABLE mart.fact_station_status_snapshot AS
SELECT *
FROM staging.stg_station_status_snapshot;

CREATE OR REPLACE TABLE mart.fact_free_bike_snapshot AS
SELECT *
FROM staging.stg_free_bike_snapshot;

CREATE OR REPLACE TABLE mart.fact_system_alerts AS
SELECT *
FROM staging.stg_system_alerts;

CREATE OR REPLACE TABLE mart.mart_trip_station_mapping_quality AS
SELECT
    COUNT(*) AS fact_trip_count,
    COUNT_IF(has_start_station_id) AS trips_with_start_station_id,
    COUNT_IF(has_end_station_id) AS trips_with_end_station_id,
    COUNT_IF(has_start_station_id AND start_station_mapped_to_gbfs) AS start_station_mapped_trip_count,
    COUNT_IF(has_end_station_id AND end_station_mapped_to_gbfs) AS end_station_mapped_trip_count,
    COUNT_IF(has_start_station_id AND start_station_mapped_to_gbfs = false) AS start_station_unmapped_trip_count,
    COUNT_IF(has_end_station_id AND end_station_mapped_to_gbfs = false) AS end_station_unmapped_trip_count,
    COUNT(DISTINCT source_start_station_id) FILTER (WHERE has_start_station_id) AS distinct_source_start_station_id_count,
    COUNT(DISTINCT source_end_station_id) FILTER (WHERE has_end_station_id) AS distinct_source_end_station_id_count,
    COUNT(DISTINCT source_start_station_id) FILTER (
        WHERE has_start_station_id AND start_station_mapped_to_gbfs = false
    ) AS distinct_unmapped_source_start_station_id_count,
    COUNT(DISTINCT source_end_station_id) FILTER (
        WHERE has_end_station_id AND end_station_mapped_to_gbfs = false
    ) AS distinct_unmapped_source_end_station_id_count
FROM mart.fact_trips;
