CREATE OR REPLACE TABLE staging.stg_trips AS
WITH typed AS (
    SELECT
        nullif(trim(ride_id), '') AS ride_id,
        lower(nullif(trim(rideable_type), '')) AS rideable_type,
        try_cast(started_at AS TIMESTAMP) AS started_at_local,
        try_cast(ended_at AS TIMESTAMP) AS ended_at_local,
        nullif(trim(start_station_name), '') AS start_station_name,
        nullif(trim(start_station_id), '') AS start_station_id,
        nullif(trim(end_station_name), '') AS end_station_name,
        nullif(trim(end_station_id), '') AS end_station_id,
        try_cast(nullif(trim(start_lat), '') AS DOUBLE) AS start_lat,
        try_cast(nullif(trim(start_lng), '') AS DOUBLE) AS start_lng,
        try_cast(nullif(trim(end_lat), '') AS DOUBLE) AS end_lat,
        try_cast(nullif(trim(end_lng), '') AS DOUBLE) AS end_lng,
        lower(nullif(trim(member_casual), '')) AS member_casual,
        source_file_name,
        source_file_path,
        source_file_period,
        try_strptime(source_file_period || '01', '%Y%m%d')::DATE AS source_file_month,
        loaded_at_utc,
        row_number() OVER (
            PARTITION BY nullif(trim(ride_id), '')
            ORDER BY source_file_path
        ) AS ride_record_number
    FROM raw.historical_trips
),
enriched AS (
    SELECT
        *,
        date_diff('second', started_at_local, ended_at_local) / 60.0 AS trip_duration_minutes
    FROM typed
)
SELECT
    ride_id,
    rideable_type,
    started_at_local,
    ended_at_local,
    trip_duration_minutes,
    CAST(started_at_local AS DATE) AS started_date,
    CAST(date_trunc('month', started_at_local) AS DATE) AS started_month,
    CAST(date_part('year', started_at_local) AS INTEGER) AS started_year,
    CAST(date_part('month', started_at_local) AS INTEGER) AS started_month_number,
    CAST(date_part('hour', started_at_local) AS INTEGER) AS started_hour,
    CAST(date_part('dayofweek', started_at_local) AS INTEGER) AS started_day_of_week_number,
    strftime(started_at_local, '%A') AS started_day_of_week_name,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    source_file_name,
    source_file_path,
    source_file_period,
    source_file_month,
    loaded_at_utc,
    ride_record_number,
    ride_id IS NOT NULL AND ride_record_number > 1 AS is_duplicate_ride_id,
    started_at_local IS NOT NULL
        AND ended_at_local IS NOT NULL
        AND ended_at_local > started_at_local AS has_valid_duration,
    trip_duration_minutes <= 1 AS is_short_trip,
    trip_duration_minutes >= 1440 AS is_over_24h,
    start_station_id IS NOT NULL AS has_start_station_id,
    end_station_id IS NOT NULL AS has_end_station_id,
    start_lat BETWEEN -90 AND 90 AND start_lng BETWEEN -180 AND 180 AS has_valid_start_coordinates,
    end_lat BETWEEN -90 AND 90 AND end_lng BETWEEN -180 AND 180 AS has_valid_end_coordinates,
    start_station_id IS NOT NULL AND end_station_id IS NOT NULL AS is_station_trip,
    ride_id IS NOT NULL
        AND ride_record_number = 1
        AND started_at_local IS NOT NULL
        AND ended_at_local IS NOT NULL
        AND ended_at_local > started_at_local
        AND trip_duration_minutes > 1
        AND trip_duration_minutes < 1440 AS is_analytics_eligible
FROM enriched;
