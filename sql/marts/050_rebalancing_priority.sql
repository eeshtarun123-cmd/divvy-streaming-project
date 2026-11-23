CREATE OR REPLACE TABLE mart.mart_rebalancing_priority AS
WITH snapshot_context AS (
    SELECT
        max(snapshot_at_utc) AS snapshot_at_utc,
        CAST(date_part('hour', max(snapshot_at_utc) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') AS INTEGER) AS snapshot_hour_local,
        CAST(date_part('dayofweek', max(snapshot_at_utc) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') AS INTEGER) AS snapshot_day_of_week_local
    FROM mart.mart_station_pressure
),
same_hour_demand AS (
    SELECT demand.*
    FROM mart.mart_hourly_demand AS demand
    INNER JOIN snapshot_context AS context
        ON demand.event_hour = context.snapshot_hour_local
        AND demand.event_day_of_week_number = context.snapshot_day_of_week_local
),
demand_scores AS (
    SELECT
        station_id,
        start_trip_count,
        end_trip_count,
        net_station_flow,
        CASE
            WHEN max(start_trip_count) OVER () > 0
                THEN start_trip_count::DOUBLE / max(start_trip_count) OVER ()
            ELSE 0
        END AS departure_demand_score,
        CASE
            WHEN max(end_trip_count) OVER () > 0
                THEN end_trip_count::DOUBLE / max(end_trip_count) OVER ()
            ELSE 0
        END AS arrival_demand_score
    FROM same_hour_demand
),
active_station_alerts AS (
    SELECT
        pressure.station_id,
        COUNT(alerts.alert_id) AS active_alert_count
    FROM mart.mart_station_pressure AS pressure
    LEFT JOIN mart.mart_active_alerts AS alerts
        ON alerts.station_ids_json IS NOT NULL
        AND contains(alerts.station_ids_json, '"' || pressure.station_id || '"')
    GROUP BY 1
)
SELECT
    pressure.snapshot_at_utc,
    pressure.station_id,
    pressure.station_name,
    pressure.lat,
    pressure.lon,
    pressure.region_id,
    pressure.station_capacity,
    pressure.available_bikes,
    pressure.available_docks,
    pressure.bike_availability_rate,
    pressure.dock_availability_rate,
    pressure.empty_pressure_score,
    pressure.full_pressure_score,
    pressure.overall_pressure_score,
    pressure.empty_risk_flag,
    pressure.full_risk_flag,
    pressure.station_pressure_category,
    pressure.station_pressure_category AS risk_category,
    pressure.station_active_flag AS is_ranking_eligible,
    coalesce(demand.start_trip_count, 0) AS same_hour_start_trip_count,
    coalesce(demand.end_trip_count, 0) AS same_hour_end_trip_count,
    coalesce(demand.net_station_flow, 0) AS same_hour_net_station_flow,
    coalesce(demand.departure_demand_score, 0) AS same_hour_departure_demand_score,
    coalesce(demand.arrival_demand_score, 0) AS same_hour_arrival_demand_score,
    coalesce(alerts.active_alert_count, 0) AS active_alert_count,
    coalesce(pressure.overall_pressure_score, 0) AS live_pressure_component,
    CASE
        WHEN coalesce(1 - pressure.bike_availability_rate, 0) >= coalesce(1 - pressure.dock_availability_rate, 0)
            THEN coalesce(demand.departure_demand_score, 0)
        ELSE coalesce(demand.arrival_demand_score, 0)
    END AS historical_demand_component,
    CASE WHEN coalesce(alerts.active_alert_count, 0) > 0 THEN 1 ELSE 0 END AS active_alert_component,
    CASE WHEN pressure.station_active_flag THEN
        round(
            100 * (
                0.60 * coalesce(pressure.overall_pressure_score, 0)
                + 0.30 * CASE
                    WHEN coalesce(1 - pressure.bike_availability_rate, 0) >= coalesce(1 - pressure.dock_availability_rate, 0)
                        THEN coalesce(demand.departure_demand_score, 0)
                    ELSE coalesce(demand.arrival_demand_score, 0)
                END
                + 0.10 * CASE WHEN coalesce(alerts.active_alert_count, 0) > 0 THEN 1 ELSE 0 END
            ),
            2
        )
    END AS rebalancing_priority_score,
    CASE
        WHEN pressure.station_active_flag = false THEN 'station_inactive'
        WHEN coalesce(1 - pressure.bike_availability_rate, 0) >= coalesce(1 - pressure.dock_availability_rate, 0)
            AND coalesce(1 - pressure.bike_availability_rate, 0) >= 0.75 THEN 'add_bikes'
        WHEN coalesce(1 - pressure.dock_availability_rate, 0) > coalesce(1 - pressure.bike_availability_rate, 0)
            AND coalesce(1 - pressure.dock_availability_rate, 0) >= 0.75 THEN 'remove_bikes'
        ELSE 'monitor'
    END AS recommended_action
FROM mart.mart_station_pressure AS pressure
LEFT JOIN demand_scores AS demand
    ON pressure.station_id = demand.station_id
LEFT JOIN active_station_alerts AS alerts
    ON pressure.station_id = alerts.station_id;
