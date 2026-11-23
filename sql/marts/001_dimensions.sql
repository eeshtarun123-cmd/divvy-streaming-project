CREATE OR REPLACE TABLE mart.dim_station AS
SELECT
    station_id,
    station_name,
    short_name,
    address,
    lat,
    lon,
    station_capacity,
    region_id,
    rental_uri_android,
    rental_uri_ios,
    has_valid_capacity,
    has_valid_coordinates,
    ingested_at_utc AS dimension_loaded_from_snapshot_at_utc
FROM staging.stg_station;

CREATE OR REPLACE TABLE mart.dim_vehicle_type AS
SELECT
    vehicle_type_id,
    form_factor,
    propulsion_type,
    vehicle_type_name,
    max_range_meters,
    ingested_at_utc AS dimension_loaded_from_snapshot_at_utc
FROM staging.stg_vehicle_type;

CREATE OR REPLACE TABLE mart.dim_station_identifier AS
WITH current_station_name_bridge AS (
    SELECT
        lower(trim(station_name)) AS normalized_station_name,
        min(station_id) AS station_id
    FROM mart.dim_station
    WHERE station_name IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) = 1
),
historical_station_identifier_candidates AS (
    SELECT DISTINCT
        nullif(trim(start_station_id), '') AS station_identifier,
        lower(trim(start_station_name)) AS normalized_station_name
    FROM staging.stg_trips
    WHERE nullif(trim(start_station_id), '') IS NOT NULL
      AND nullif(trim(start_station_name), '') IS NOT NULL

    UNION

    SELECT DISTINCT
        nullif(trim(end_station_id), '') AS station_identifier,
        lower(trim(end_station_name)) AS normalized_station_name
    FROM staging.stg_trips
    WHERE nullif(trim(end_station_id), '') IS NOT NULL
      AND nullif(trim(end_station_name), '') IS NOT NULL
),
historical_station_identifier_bridge AS (
    SELECT
        candidates.station_identifier,
        min(current_station_name_bridge.station_id) AS station_id
    FROM historical_station_identifier_candidates AS candidates
    INNER JOIN current_station_name_bridge
        ON candidates.normalized_station_name = current_station_name_bridge.normalized_station_name
    GROUP BY 1
    HAVING COUNT(DISTINCT current_station_name_bridge.station_id) = 1
),
station_identifier_raw AS (
    SELECT
        station_id,
        station_id AS station_identifier,
        'gbfs_station_id' AS station_identifier_type,
        1 AS match_priority
    FROM mart.dim_station
    WHERE station_id IS NOT NULL

    UNION ALL

    SELECT
        station_id,
        short_name AS station_identifier,
        'gbfs_short_name' AS station_identifier_type,
        2 AS match_priority
    FROM mart.dim_station
    WHERE short_name IS NOT NULL
      AND short_name <> station_id

    UNION ALL

    SELECT
        station_id,
        station_identifier,
        'historical_station_id_name_match' AS station_identifier_type,
        3 AS match_priority
    FROM historical_station_identifier_bridge
),
ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY station_identifier
            ORDER BY match_priority, station_id
        ) AS station_identifier_row_number
    FROM station_identifier_raw
)
SELECT
    station_id,
    station_identifier,
    station_identifier_type
FROM ranked
WHERE station_identifier_row_number = 1;

CREATE OR REPLACE TABLE mart.dim_date AS
WITH date_source AS (
    SELECT started_date AS date_day
    FROM staging.stg_trips
    WHERE started_date IS NOT NULL

    UNION ALL

    SELECT snapshot_date AS date_day
    FROM staging.stg_station_status_snapshot
    WHERE snapshot_date IS NOT NULL
),
bounds AS (
    SELECT
        min(date_day) AS min_date,
        max(date_day) AS max_date
    FROM date_source
),
calendar AS (
    SELECT CAST(date_day AS DATE) AS date_day
    FROM bounds,
    generate_series(min_date, max_date, INTERVAL 1 DAY) AS dates(date_day)
)
SELECT
    CAST(strftime(date_day, '%Y%m%d') AS INTEGER) AS date_key,
    date_day,
    CAST(date_part('year', date_day) AS INTEGER) AS year_number,
    CAST(date_part('quarter', date_day) AS INTEGER) AS quarter_number,
    CAST(date_part('month', date_day) AS INTEGER) AS month_number,
    strftime(date_day, '%B') AS month_name,
    CAST(date_part('day', date_day) AS INTEGER) AS day_of_month,
    CAST(date_part('day', date_day) AS INTEGER) AS day_number,
    CAST(date_part('dayofweek', date_day) AS INTEGER) AS day_of_week_number,
    strftime(date_day, '%A') AS day_of_week_name,
    CAST(date_trunc('week', date_day) AS DATE) AS week_start_date,
    CAST(date_part('dayofweek', date_day) AS INTEGER) IN (0, 6) AS is_weekend,
    strftime(date_day, '%Y-%m') AS year_month
FROM calendar;

CREATE OR REPLACE TABLE mart.dim_time AS
SELECT
    hour_of_day AS hour_key,
    hour_of_day AS hour_number,
    hour_of_day,
    lpad(CAST(hour_of_day AS VARCHAR), 2, '0') || ':00' AS hour_label,
    CASE
        WHEN hour_of_day BETWEEN 0 AND 4 THEN 'overnight'
        WHEN hour_of_day BETWEEN 5 AND 11 THEN 'morning'
        WHEN hour_of_day BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN hour_of_day BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'night'
    END AS daypart,
    CASE
        WHEN hour_of_day BETWEEN 0 AND 5 THEN 'overnight'
        WHEN hour_of_day BETWEEN 6 AND 9 THEN 'morning_commute'
        WHEN hour_of_day BETWEEN 10 AND 15 THEN 'midday'
        WHEN hour_of_day BETWEEN 16 AND 19 THEN 'evening_commute'
        ELSE 'night'
    END AS time_period,
    hour_of_day BETWEEN 7 AND 9 OR hour_of_day BETWEEN 16 AND 18 AS is_commute_peak,
    hour_of_day BETWEEN 6 AND 9 OR hour_of_day BETWEEN 16 AND 19 AS is_commute_period
FROM range(0, 24) AS hours(hour_of_day);
