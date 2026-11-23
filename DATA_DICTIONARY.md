# Data Dictionary

## Operational Tables

| Table | Purpose |
| --- | --- |
| `ops.pipeline_run_log` | Step-level run audit log with status, timing, rows affected, and error messages. |
| `ops.validation_results` | Raw, staging, and mart validation results by run. |
| `ops.validation_run_summary` | Validation summary view by run and layer with check counts, warning counts, error failures, and run status. |
| `ops.source_file_manifest` | Source file checkpoint table with file size, checksum, processed status, row count, and event-time range. |
| `ops.pipeline_watermarks` | Latest processed source progress for GBFS feeds and historical trips. |

## Raw Tables

| Table | Purpose |
| --- | --- |
| `raw.historical_trips` | Raw historical Divvy trip CSV rows with source file lineage. |
| `raw.historical_trip_file_profile` | Per-file row counts, date ranges, duplicates, missing station counts, and duration profile metrics. |
| `raw.gbfs_snapshot_files` | One row per raw GBFS JSON snapshot file with raw response JSON. |
| `raw.gbfs_feed_index` | Flattened GBFS feed index by language and feed. |
| `raw.gbfs_station_information` | Raw station metadata from GBFS station information. |
| `raw.gbfs_station_status` | Raw live station availability snapshots. |
| `raw.gbfs_station_vehicle_types_available` | Flattened vehicle-type availability by station snapshot. |
| `raw.gbfs_free_bike_status` | Raw free-floating bike/scooter availability snapshots. |
| `raw.gbfs_vehicle_types` | Raw GBFS vehicle type metadata. |
| `raw.gbfs_system_alerts` | Raw service alerts. May be empty when no alerts are active. |

## Staging Tables

| Table | Key Columns | Purpose |
| --- | --- | --- |
| `staging.stg_trips` | `ride_id`, `started_at_local`, `ended_at_local`, `trip_duration_minutes`, `is_analytics_eligible` | Typed and quality-flagged historical trips. |
| `staging.stg_station` | `station_id`, `short_name`, `station_name`, `lat`, `lon`, `station_capacity` | Latest station dimension candidate from GBFS metadata. |
| `staging.stg_vehicle_type` | `vehicle_type_id`, `form_factor`, `propulsion_type` | Latest vehicle type dimension candidate. |
| `staging.stg_station_status_snapshot` | `station_id`, `snapshot_at_utc`, `available_bikes`, `available_docks`, pressure scores | Typed live station availability snapshots joined to station capacity. |
| `staging.stg_station_vehicle_type_status_snapshot` | `station_id`, `snapshot_at_utc`, `vehicle_type_id`, `available_count` | Vehicle availability by station and type. |
| `staging.stg_free_bike_snapshot` | `bike_id`, `snapshot_at_utc`, `lat`, `lon`, `vehicle_type_id` | Typed free-floating vehicle snapshots. |
| `staging.stg_system_alerts` | `alert_id`, `alert_type`, `alert_active_flag` | Typed service alerts. |
| `staging.stg_gbfs_feed_index` | `language`, `feed_name`, `feed_url` | GBFS feed inventory. |

## Mart Tables

| Table | Grain | Purpose |
| --- | --- | --- |
| `mart.dim_station` | One row per current GBFS station | Station dimension for maps and station joins. |
| `mart.dim_station_identifier` | One row per station identifier | Bridge from historical CHI-style IDs and GBFS IDs to canonical station IDs. |
| `mart.dim_vehicle_type` | One row per vehicle type | Vehicle type dimension. |
| `mart.dim_date` | One row per date in historical trip range | Date dimension for Power BI calendar filtering. |
| `mart.dim_time` | One row per hour of day | Hour/daypart dimension for Power BI time filtering. |
| `mart.fact_trips` | One row per analytics-eligible historical trip | Clean trip fact table. |
| `mart.fact_station_status_snapshot` | One row per station snapshot | Live station availability fact table. |
| `mart.fact_free_bike_snapshot` | One row per free-floating vehicle snapshot | Free-floating vehicle fact table. |
| `mart.fact_system_alerts` | One row per alert snapshot | Service alert fact table. |
| `mart.mart_station_pressure` | One row per station for latest snapshot | Dashboard-ready live pressure table. |
| `mart.mart_rebalancing_priority` | One row per station for latest snapshot | Operational rebalancing recommendation and priority score. |
| `mart.mart_hourly_demand` | Station + day-of-week + hour | Historical departures and arrivals by their respective local event times (`event_day_of_week_number`, `event_hour`). |
| `mart.mart_station_demand` | One row per station | Aggregate station demand and net flow. |
| `mart.mart_station_trip_patterns` | Station + month + day-of-week + hour | Historical departures and arrivals by local event month (`event_month`), weekday, and hour. |
| `mart.mart_member_casual_usage` | Month + rider type + rideable type | Rider and vehicle usage summary. |
| `mart.mart_vehicle_availability` | One row per vehicle type | Latest station and free-floating vehicle availability. |
| `mart.mart_active_alerts` | One row per active alert | Active service issues in the latest alert feed snapshot. An empty latest snapshot produces zero rows. |
| `mart.mart_system_overview` | One row per latest system snapshot | KPI summary for Power BI overview page. |
| `mart.mart_trip_station_mapping_quality` | One row | Mapping quality metrics between historical station IDs and live GBFS station IDs. |
| `mart.mart_pipeline_health` | One row for the latest pipeline run | Pipeline observability mart for run status, validation status, manifest status, and watermark freshness. |

## mart.dim_date

Purpose: Calendar dimension for Power BI date filtering and time-series analysis.

Columns:
- `date_day`
- `year_number`
- `quarter_number`
- `month_number`
- `month_name`
- `day_number`
- `day_of_week_number`
- `day_of_week_name`
- `is_weekend`
- `year_month`

Grain: One row per date.

## mart.dim_time

Purpose: Hour-level time dimension for trip demand and live station snapshot analysis.

Columns:
- `hour_number`
- `hour_label`
- `time_period`
- `is_commute_period`

Grain: One row per hour of day.

## mart.mart_pipeline_health

Purpose: Pipeline monitoring mart showing latest run health, validation status, manifest status, and watermark freshness.

Columns:
- `run_id`
- `pipeline_started_at_utc`
- `pipeline_ended_at_utc`
- `total_duration_seconds`
- `total_steps`
- `successful_steps`
- `failed_steps`
- `total_validation_checks`
- `passed_validation_checks`
- `error_validation_failures`
- `warning_validation_failures`
- `total_manifest_files`
- `processed_files`
- `skipped_files`
- `total_watermarks`
- `pipeline_health_status`

Grain: One row for the latest pipeline run.

For the actionable station ranking, filter `mart.mart_rebalancing_priority` to `is_ranking_eligible = true`. Inactive stations remain available for the map and have a null priority score.
