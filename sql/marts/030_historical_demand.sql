CREATE OR REPLACE TABLE mart.mart_hourly_demand AS
WITH start_events AS (
    SELECT
        start_gbfs_station_id AS station_id,
        started_day_of_week_number AS event_day_of_week_number,
        started_day_of_week_name AS event_day_of_week_name,
        started_hour AS event_hour,
        COUNT(*) AS start_trip_count,
        AVG(trip_duration_minutes) AS avg_start_trip_duration_minutes,
        COUNT_IF(member_casual = 'member') AS member_start_trip_count,
        COUNT_IF(member_casual = 'casual') AS casual_start_trip_count,
        COUNT_IF(rideable_type = 'classic_bike') AS classic_start_trip_count,
        COUNT_IF(rideable_type = 'electric_bike') AS electric_start_trip_count
    FROM mart.fact_trips
    WHERE start_gbfs_station_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
end_events AS (
    SELECT
        end_gbfs_station_id AS station_id,
        CAST(date_part('dayofweek', ended_at_local) AS INTEGER) AS event_day_of_week_number,
        strftime(ended_at_local, '%A') AS event_day_of_week_name,
        CAST(date_part('hour', ended_at_local) AS INTEGER) AS event_hour,
        COUNT(*) AS end_trip_count
    FROM mart.fact_trips
    WHERE end_gbfs_station_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT
    coalesce(start_events.station_id, end_events.station_id) AS station_id,
    station.station_name,
    coalesce(start_events.event_day_of_week_number, end_events.event_day_of_week_number) AS event_day_of_week_number,
    coalesce(start_events.event_day_of_week_name, end_events.event_day_of_week_name) AS event_day_of_week_name,
    coalesce(start_events.event_hour, end_events.event_hour) AS event_hour,
    coalesce(start_events.start_trip_count, 0) AS start_trip_count,
    coalesce(end_events.end_trip_count, 0) AS end_trip_count,
    coalesce(end_events.end_trip_count, 0) - coalesce(start_events.start_trip_count, 0) AS net_station_flow,
    start_events.avg_start_trip_duration_minutes,
    coalesce(start_events.member_start_trip_count, 0) AS member_start_trip_count,
    coalesce(start_events.casual_start_trip_count, 0) AS casual_start_trip_count,
    coalesce(start_events.classic_start_trip_count, 0) AS classic_start_trip_count,
    coalesce(start_events.electric_start_trip_count, 0) AS electric_start_trip_count
FROM start_events
FULL OUTER JOIN end_events
    ON start_events.station_id = end_events.station_id
    AND start_events.event_day_of_week_number = end_events.event_day_of_week_number
    AND start_events.event_hour = end_events.event_hour
LEFT JOIN mart.dim_station AS station
    ON coalesce(start_events.station_id, end_events.station_id) = station.station_id;

CREATE OR REPLACE TABLE mart.mart_station_demand AS
WITH start_events AS (
    SELECT
        start_gbfs_station_id AS station_id,
        COUNT(*) AS start_trip_count,
        COUNT(DISTINCT started_date) AS active_start_day_count,
        AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
        COUNT_IF(member_casual = 'member') AS member_start_trip_count,
        COUNT_IF(member_casual = 'casual') AS casual_start_trip_count,
        COUNT_IF(rideable_type = 'classic_bike') AS classic_start_trip_count,
        COUNT_IF(rideable_type = 'electric_bike') AS electric_start_trip_count
    FROM mart.fact_trips
    WHERE start_gbfs_station_id IS NOT NULL
    GROUP BY 1
),
end_events AS (
    SELECT
        end_gbfs_station_id AS station_id,
        COUNT(*) AS end_trip_count,
        COUNT(DISTINCT CAST(ended_at_local AS DATE)) AS active_end_day_count
    FROM mart.fact_trips
    WHERE end_gbfs_station_id IS NOT NULL
    GROUP BY 1
)
SELECT
    coalesce(start_events.station_id, end_events.station_id) AS station_id,
    station.station_name,
    station.station_capacity,
    coalesce(start_events.start_trip_count, 0) AS start_trip_count,
    coalesce(end_events.end_trip_count, 0) AS end_trip_count,
    coalesce(start_events.start_trip_count, 0) + coalesce(end_events.end_trip_count, 0) AS total_station_trip_events,
    coalesce(end_events.end_trip_count, 0) - coalesce(start_events.start_trip_count, 0) AS net_station_flow,
    start_events.avg_trip_duration_minutes,
    CASE
        WHEN coalesce(start_events.active_start_day_count, 0) > 0
            THEN start_events.start_trip_count::DOUBLE / start_events.active_start_day_count
        ELSE NULL
    END AS avg_daily_start_trips,
    CASE
        WHEN coalesce(end_events.active_end_day_count, 0) > 0
            THEN end_events.end_trip_count::DOUBLE / end_events.active_end_day_count
        ELSE NULL
    END AS avg_daily_end_trips,
    coalesce(start_events.member_start_trip_count, 0) AS member_start_trip_count,
    coalesce(start_events.casual_start_trip_count, 0) AS casual_start_trip_count,
    coalesce(start_events.classic_start_trip_count, 0) AS classic_start_trip_count,
    coalesce(start_events.electric_start_trip_count, 0) AS electric_start_trip_count
FROM start_events
FULL OUTER JOIN end_events
    ON start_events.station_id = end_events.station_id
LEFT JOIN mart.dim_station AS station
    ON coalesce(start_events.station_id, end_events.station_id) = station.station_id;

CREATE OR REPLACE TABLE mart.mart_station_trip_patterns AS
WITH station_month_hour AS (
    SELECT
        start_gbfs_station_id AS station_id,
        started_month AS event_month,
        started_day_of_week_number AS event_day_of_week_number,
        started_day_of_week_name AS event_day_of_week_name,
        started_hour AS event_hour,
        COUNT(*) AS start_trip_count,
        0 AS end_trip_count
    FROM mart.fact_trips
    WHERE start_gbfs_station_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5

    UNION ALL

    SELECT
        end_gbfs_station_id AS station_id,
        CAST(date_trunc('month', ended_at_local) AS DATE) AS event_month,
        CAST(date_part('dayofweek', ended_at_local) AS INTEGER) AS event_day_of_week_number,
        strftime(ended_at_local, '%A') AS event_day_of_week_name,
        CAST(date_part('hour', ended_at_local) AS INTEGER) AS event_hour,
        0 AS start_trip_count,
        COUNT(*) AS end_trip_count
    FROM mart.fact_trips
    WHERE end_gbfs_station_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    events.station_id,
    station.station_name,
    events.event_month,
    events.event_day_of_week_number,
    events.event_day_of_week_name,
    events.event_hour,
    SUM(events.start_trip_count) AS start_trip_count,
    SUM(events.end_trip_count) AS end_trip_count,
    SUM(events.end_trip_count) - SUM(events.start_trip_count) AS net_station_flow
FROM station_month_hour AS events
LEFT JOIN mart.dim_station AS station
    ON events.station_id = station.station_id
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE TABLE mart.mart_member_casual_usage AS
SELECT
    started_month,
    member_casual,
    rideable_type,
    COUNT(*) AS trip_count,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    approx_quantile(trip_duration_minutes, 0.5) AS median_trip_duration_minutes,
    COUNT_IF(has_start_station_id = false) AS missing_start_station_trip_count,
    COUNT_IF(has_end_station_id = false) AS missing_end_station_trip_count,
    COUNT_IF(has_start_station_id AND start_station_mapped_to_gbfs = false) AS unmapped_start_station_trip_count,
    COUNT_IF(has_end_station_id AND end_station_mapped_to_gbfs = false) AS unmapped_end_station_trip_count
FROM mart.fact_trips
GROUP BY 1, 2, 3;
