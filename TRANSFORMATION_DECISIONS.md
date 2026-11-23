# Transformation Decisions

This document records the approved transformation choices for the Divvy mobility pipeline.

## Source Handling

- Uploaded CSV and GBFS JSON files are treated as immutable source files.
- Normal pipeline runs process only new or changed source files using `ops.source_file_manifest`.
- `--full-refresh` rebuilds raw tables from all available source files.
- File names are retained for lineage, but reporting dates are derived from the data itself.
- Historical trip ZIP files are extracted into `data/raw/tripdata/` when present.

## Historical Trips

- Keep all raw trip rows in `raw.historical_trips`.
- Keep all parsed trip rows in `staging.stg_trips` with quality flags.
- Exclude from `mart.fact_trips`:
  - duplicate `ride_id`
  - missing or invalid timestamps
  - trips at or below 1 minute
  - trips at or above 24 hours (`trip_duration_minutes >= 1440`)
- Do not impute missing station IDs or coordinates.
- Trips with missing station IDs remain in staging but do not contribute to station-level demand marts.
- `started_month`, `started_hour`, and day-of-week fields come from `started_at_local`.
- Station demand uses departure time for departures and arrival time for arrivals. Hourly and monthly station patterns use local `event_*` fields. Net station flow is arrivals minus departures in the same event bucket.
- Station arrival active days use the local arrival date; rider-mix reporting remains grouped by departure month.

## 24-Hour Trip Rule

Decision: Trips with duration greater than or equal to 24 hours are excluded from analytics-ready mart tables.

Rule:

```sql
trip_duration_minutes > 1
AND trip_duration_minutes < 1440
```

Reason: Trips equal to 1440 minutes represent exactly 24 hours and are outside the intended operational trip analysis window.

Layer behavior:
- Staging keeps these records and flags them with `is_over_24h`.
- `mart.fact_trips` excludes them through `is_analytics_eligible`.

## Station Identity

- Live GBFS station IDs are the canonical station IDs for live operational marts.
- Historical trip station IDs may use CHI-style identifiers.
- `mart.dim_station_identifier` bridges historical identifiers to GBFS station IDs using both `station_id` and `short_name`.
- Historical station mappings are warnings, not failures, because old or retired stations may not exist in current GBFS metadata.

## Live GBFS

- All six GBFS feeds are ingested:
  - feed index
  - station information
  - station status
  - free bike status
  - system alerts
  - vehicle types
- `system_alerts` may legitimately contain zero rows.
- Nested API structures are flattened into analytic tables where useful, including station vehicle-type availability.
- GBFS epoch timestamps are converted to UTC timestamps.

## Pressure And Rebalancing

- `bike_availability_rate` = available bikes / capacity.
- `dock_availability_rate` = available docks / capacity.
- `station_pressure_score` is retained for compatibility with the original guide and mirrors bike availability.
- `empty_pressure_score` = max(1 - bike availability rate, 0).
- `full_pressure_score` = max(1 - dock availability rate, 0).
- `overall_pressure_score` = greatest(empty pressure, full pressure).
- Rebalancing priority combines:
  - 60% live pressure
  - 30% same-hour historical demand
  - 10% active alert impact

## Dock Health and Nearby Parking

- The operations dashboard measures how reliably riders can rent and return bikes at physical docking stations across Chicago. Nearby cable-lock parking is a fallback and can reveal pressure on a dock that has run out of return space.
- Track sustained zero-open-dock periods at active physical stations alongside typical return demand for the same local weekday and hour. Keep public racks separate from physical dock utilization. Nearby rack capacity does not count as open docks at the station.
- Compare e-bike arrivals at nearby racks during full-dock periods with comparable periods when docks were open. If only station snapshots are available, use changes in nearby rack e-bike counts as a weaker signal because pickups and fleet moves also change inventory. Treat missing observations as gaps.
- Label this signal possible return overflow. Evaluate rider impact from the perspective of a non-member who has to use paid parking after finding a full dock. Nearby rack activity can include approved fee-free locations and voluntary parking, so treat it as evidence of potential fee exposure. The feed does not identify membership, attempted returns, or actual charges. Report possible overflow and any explicitly labeled non-member cost scenario separately. See [Divvy e-bike parking guidance](https://help.divvybikes.com/hc/en-us/articles/360046649911-Riding-with-an-ebike).
- Use the collected week to assess persistence and choose demand thresholds before changing operational risk flags or ranking.

## Date and Time Dimensions

Decision: Create `mart.dim_date` and `mart.dim_time` for Power BI modeling.

Reason: Separate dimensions improve filtering, dashboard usability, and star-schema design.

## Pipeline Health Mart

Decision: Create `mart.mart_pipeline_health` from ops tables.

Reason: The project should show not only business analytics, but also pipeline observability through run status, validation results, file manifest status, and watermark freshness.

## Validation Behavior

- ERROR validation failures stop the pipeline.
- WARNING validation failures are recorded and the pipeline continues.
- INFO-style profile metrics are captured through profile, manifest, watermark, and validation tables.
- Incremental runs mark unchanged source files as `SKIPPED_UNCHANGED` in `ops.source_file_manifest`.
- Staging and mart transformation batches run in transactions and roll back the layer if a SQL file fails.
- Pipeline health step counts are based on distinct logical steps, not individual status-event rows.
