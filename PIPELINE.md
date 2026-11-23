# Divvy Analytics Pipeline

This project loads Divvy GBFS live snapshots and uploaded Divvy historical trip CSVs into a local DuckDB warehouse, transforms them into staging and mart tables, and exports all layers to Parquet for BI tools and future Databricks migration.

## Run The Pipeline

```bash
.venv/bin/python scripts/run_pipeline.py
```

To rebuild raw tables from every source file:

```bash
.venv/bin/python scripts/run_pipeline.py --full-refresh
```

To fetch fresh GBFS API snapshots before loading the warehouse:

```bash
.venv/bin/python scripts/run_pipeline.py --fetch-live
```

For a fresh API pull plus complete rebuild:

```bash
.venv/bin/python scripts/run_pipeline.py --fetch-live --full-refresh
```

For controlled lightweight polling, fetch raw GBFS JSON only:

```bash
.venv/bin/python scripts/poll_gbfs_live.py --interval-seconds 60 --duration-minutes 30
```

## Outputs

- DuckDB warehouse: `data/warehouse/divvy.duckdb`
- Raw Parquet: `data/processed/raw/`
- Staging Parquet: `data/processed/staging/`
- Mart Parquet: `data/processed/mart/`
- File logs: `logs/pipeline.log`
- Audit logs: `ops.pipeline_run_log` and convenience view `pipeline_run_log`
- Validation results: `ops.validation_results` and convenience view `validation_results`
- Validation summaries: `ops.validation_run_summary` and convenience view `validation_run_summary`
- Source checkpoints: `ops.source_file_manifest` and convenience view `source_file_manifest`
- Watermarks: `ops.pipeline_watermarks` and convenience view `pipeline_watermarks`

## Pipeline Order

1. Optional GBFS API fetch
2. Historical trip ingestion
3. GBFS raw JSON ingestion
4. Raw validation
5. Staging transforms
6. Staging validation
7. Mart transforms
8. Mart validation
9. Pipeline health refresh
10. Parquet export
11. Final pipeline health synchronization and health Parquet refresh

## Pipeline Layers

Raw tables preserve the uploaded/API source data with lineage fields.

Staging tables parse data types, flatten nested API structures, add quality flags, and derive reporting fields from the data itself. Historical trip months are derived from `started_at_local`, not from the CSV filename.

Mart tables are analytics-ready tables for Power BI or another BI layer:

- `mart.dim_station`
- `mart.dim_station_identifier`
- `mart.dim_vehicle_type`
- `mart.dim_date`
- `mart.dim_time`
- `mart.fact_trips`
- `mart.fact_station_status_snapshot`
- `mart.fact_free_bike_snapshot`
- `mart.fact_system_alerts`
- `mart.mart_system_overview`
- `mart.mart_station_pressure`
- `mart.mart_rebalancing_priority`
- `mart.mart_hourly_demand`
- `mart.mart_station_demand`
- `mart.mart_station_trip_patterns`
- `mart.mart_member_casual_usage`
- `mart.mart_vehicle_availability`
- `mart.mart_active_alerts`
- `mart.mart_trip_station_mapping_quality`
- `mart.mart_pipeline_health`

## Power BI Tables

Use mart tables only for the first dashboard build:

For hourly demand and station trip patterns, use `event_hour` and `event_day_of_week_number` for time filters, and `event_month` for monthly station patterns. Departures are grouped by local start time and arrivals by local end time. Use the trip start fields in `mart.fact_trips` when analyzing departure cohorts.

- `mart.mart_system_overview`
- `mart.mart_station_pressure`
- `mart.mart_rebalancing_priority`
- `mart.mart_hourly_demand`
- `mart.mart_station_demand`
- `mart.mart_member_casual_usage`
- `mart.mart_vehicle_availability`
- `mart.mart_active_alerts`
- `mart.mart_pipeline_health`
- `mart.dim_station`
- `mart.dim_vehicle_type`
- `mart.dim_date`
- `mart.dim_time`

## Checkpoints And Watermarks

The pipeline tracks source files in `ops.source_file_manifest` using file size, modified time, and SHA-256 checksum. Normal runs process only new or changed files and mark unchanged inputs as `SKIPPED_UNCHANGED`. Use `--full-refresh` when you want to rebuild from all files for a demo or model/schema change.

The pipeline tracks source progress in `ops.pipeline_watermarks`:

- GBFS watermarks store latest snapshot ingestion time, latest GBFS feed update time, and source file path per feed.
- Historical trip watermarks store latest trip start/end timestamps and latest processed source file metadata.

Staging and mart SQL files run inside layer-level DuckDB transactions. If one SQL file fails, the affected layer is rolled back instead of being left partially rebuilt.

`mart.mart_pipeline_health` counts logical pipeline steps rather than individual `STARTED` and `SUCCESS` log events. After the main Parquet export finishes, the pipeline refreshes the health mart and its Parquet file once more so both include the completed export step.

## Key Transformation Decisions

- Source files are not moved, deleted, or overwritten.
- Historical trip reporting dates come from `started_at_local`.
- Uploaded filenames are retained as lineage fields only.
- Trips at or below 1 minute and trips at or above 24 hours remain in staging with quality flags, but are excluded from `mart.fact_trips`.
- Trips with missing station IDs remain available in staging and eligible non-station marts, but are excluded from station-level demand marts.
- Historical station IDs are mapped to GBFS station IDs through `mart.dim_station_identifier`, using both GBFS `station_id` and `short_name`.
- Live station pressure uses current GBFS station status joined to station capacity from station information.
- Rebalancing priority combines live pressure, same-hour historical demand, and active alert impact.

## Tableau Atlas

After refreshing the warehouse, build the editable Tableau package:

```bash
.venv/bin/python scripts/build_tableau_atlas.py
```

The package is `data/processed/tableau/atlas/Chicago Bikeshare Atlas.twbx`.
It contains six dashboards, 37 native worksheets, compact aggregate CSV
connections, and selection actions. Hidden worksheet windows keep the published
navigation focused on the six dashboards. Upload the TWBX through Tableau Cloud
Home → New → Upload Workbook, with **Show sheets as tabs** enabled. Overwrite the
Atlas to release a revision; the original Chicago Bikeshare Analytics workbook
is separate.

The builder checks comparable-period totals, rider-hour totals, duration-bin
totals, and endpoint breakdown totals. `manifest.json` records pipeline health,
coverage, row counts, and the operations feed observation time.

Run the independent report checks before uploading a revision:

```bash
.venv/bin/python scripts/validate_tableau_atlas.py
```

The initial release passes 44 reconciliation and integrity checks. Results are
saved to `data/processed/tableau/atlas/validation_report.json`, including cleaning
sensitivity. For January–October, source-row growth is 14.663% and eligible-trip
growth is 14.835%. The 2026 exclusion rate is 2.613%. Exclusion categories overlap,
so the report calculates removed rows directly rather than summing warning counts.

The business comparison uses January–October 2025 versus January–October 2026.
Station activity uses independently matched pickup and return event timestamps
within January–October 2026. A trip whose return crosses the reporting boundary
can contribute to only one of those event totals. Calendar-day rates use all
304 days in that reporting window, including days with zero recorded events.

The operations page is a **saved snapshot**, not an automatic refresh connection.
It records both feed and station observation times. Locations with names matching
public rack, e-station, virtual, or corral are separated from positive-capacity
dock candidates. This is a documented name heuristic, not verified infrastructure
metadata. Reports more than five minutes older than the feed are omitted from
triage. The shortlist score is an exploratory 70% scarcity / 30% normalized
same-weekday-and-hour historical demand heuristic for fresh active dock candidates;
it is not a forecast, SLA, truck route, or probability. It intentionally does
not reuse the older general-purpose mart score across racks and physical docks.

The station catalog retains each location's last observed metadata, including
some locations absent from the newest station-status feed. Its count can exceed
the number of locations in the latest operations snapshot. Current catalog
capacity must not be interpreted as historical station capacity.

Research sources checked for this build:

- [Divvy system data](https://divvybikes.com/system-data): monthly anonymized trip
  history and a separate GBFS live-availability source. The official archive had
  January–October 2026 files available on November 23, 2026.
- [Divvy e-bike parking guidance](https://help.divvybikes.com/hc/en-us/articles/360046649911-Riding-with-an-ebike): virtual stations and full-station fee exceptions
  make rack activity unsuitable as a direct measure of actual fees or failed returns.
- [Tableau workbook upload](https://help.tableau.com/current/pro/desktop/en-gb/upload.htm): packaged workbooks can be uploaded to Cloud from the browser.

Trip records cannot directly establish revenue, profit, unique riders, rider
conversion, trip purpose, unmet demand, actual fees, or causal weather effects.
The short, interrupted station collection does not support weekly reliability or
sustained zero-dock durations; the operations page does not claim those metrics.

## Companion Docs

- `TRANSFORMATION_DECISIONS.md` records approved keep/drop, cleaning, validation, and business-rule decisions.
- `DATA_DICTIONARY.md` summarizes raw, staging, mart, and ops tables for review and Power BI modeling.
