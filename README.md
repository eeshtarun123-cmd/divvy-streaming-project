# Chicago Bikeshare Atlas

A Python, DuckDB, and Tableau project that follows Chicago-area Divvy bikeshare trips from source files to six dashboards. It compares ridership across matched months, explores rider and station patterns, and examines availability at a saved GBFS observation.

[Download the packaged workbook](./Chicago%20Bikeshare%20Atlas.twbx) and open it in Tableau Desktop. The six dashboard tabs share a single workbook.

## What the data shows

- Eligible rides rose from **4,916,470** to **5,645,807** across January–October 2025 and January–October 2026, an increase of **14.8%**. E-bike rides grew by 917,738 while classic-bike rides fell by 188,401.
- In the 2026 comparison window, **80.1% of pickups** and **79.4% of returns** could be attributed to stations. Station analyses use matched endpoints.
- Operations Snapshot uses the feed observed on **November 23, 2026 at 6:59 a.m. Chicago time**. Its bike and dock counts describe conditions at that observation.

## Dashboard guide

| Tab | Business question |
| --- | --- |
| Business Pulse | How much did trip volume grow, and which bike and rider segments contributed? |
| Rider Rhythm | When do members and casual riders travel, and how long are their trips? |
| Station Atlas | Where do pickups and returns concentrate, and how does activity change by station? |
| Demand and Flow | Which stations have the largest net departures or arrivals, and at what hours? |
| Operations Snapshot | Which dock candidates had few bikes or return spaces at the saved observation? |
| Evidence and Coverage | How much data survived cleaning, and which endpoints could be attributed to stations? |

## Reproduce the pipeline

1. Create a Python environment and install the dependencies.

   ```bash
   python3 -m venv .venv
   .venv/bin/python -m pip install -r requirements.txt
   ```

2. Download the official monthly trip archives for January 2025 through October 2026. Create a local `data/` directory and put the archives or extracted `*-divvy-tripdata.csv` files in it. [Divvy system data](https://divvybikes.com/system-data)
3. Run the pipeline, build the Tableau workbook, and reconcile its aggregates against the warehouse.

   ```bash
   .venv/bin/python scripts/run_pipeline.py --fetch-live
   .venv/bin/python scripts/build_tableau_atlas.py
   .venv/bin/python scripts/validate_tableau_atlas.py
   ```

4. Open `data/processed/tableau/atlas/Chicago Bikeshare Atlas.twbx` in Tableau Desktop. The package contains all six dashboards and their supporting aggregate data. The operations snapshot reflects the feed fetched during your run. Current station metadata can also change which historical endpoints are matched.

The release passed **44 workbook reconciliation and integrity checks**. The warehouse checks reported **zero errors and nine warnings**. The pipeline documentation explains the cleaning rules, station matching, and interpretation of the operations view. [Pipeline](PIPELINE.md) · [Data dictionary](DATA_DICTIONARY.md) · [Transformation decisions](TRANSFORMATION_DECISIONS.md)

This is an independent, noncommercial analysis. The underlying source data is made available under the [Divvy Data License Agreement](https://divvybikes.com/data-license-agreement).
