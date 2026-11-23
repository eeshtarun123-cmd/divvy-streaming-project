"""Reconcile the packaged BI aggregates against independent warehouse queries."""
from pathlib import Path
from datetime import datetime, timezone
import json
import xml.etree.ElementTree as E
import zipfile

import duckdb
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'data/processed/tableau/atlas'
c=duckdb.connect(str(ROOT/'data/warehouse/divvy.duckdb'),read_only=True)
results=[]
def check(name, actual, expected):
    assert actual==expected,(name,actual,expected)
    results.append({'check':name,'status':'PASS','actual':actual,'expected':expected})

def frame(name):return pd.read_csv(OUT/f'{name}.csv',dtype={'Station ID':'string'},low_memory=False)
r=frame('ridership');rh=frame('rhythm');s=frame('stations');o=frame('operations');q=frame('quality')
expected={y:n for y,n in c.execute("""SELECT started_year,count(*) FROM staging.stg_trips
 WHERE is_analytics_eligible AND started_year IN (2025,2026) AND started_month_number<=10 GROUP BY 1""").fetchall()}
for year,total in expected.items():
    check(f'{year} comparable trips from eligible staging rows',int(r.loc[r.Year==year,'Trips'].sum()),total)
    check(f'{year} comparable month count',int(r.loc[r.Year==year,'Month'].nunique()),10)
check('unique mart ride IDs',c.execute('select count(distinct ride_id) from mart.fact_trips').fetchone()[0],c.execute('select count(*) from mart.fact_trips').fetchone()[0])
check('non-positive mart trip durations',c.execute('select count(*) from mart.fact_trips where ended_at_local<=started_at_local').fetchone()[0],0)
check('rhythm total',int(rh.Trips.sum()),expected[2026])
check('duration distribution total',int(frame('duration').Trips.sum()),expected[2026])

pickup=c.execute("""select count(*) from mart.fact_trips where start_station_mapped_to_gbfs
 and started_at_local>='2026-01-01' and started_at_local<'2026-11-01'""").fetchone()[0]
returns=c.execute("""select count(*) from mart.fact_trips where end_station_mapped_to_gbfs
 and ended_at_local>='2026-01-01' and ended_at_local<'2026-11-01'""").fetchone()[0]
check('independent station pickup events',int(s.Pickups.sum()),pickup)
check('independent station return events',int(s.Returns.sum()),returns)
check('signed net flow',int(s['Net flow'].sum()),returns-pickup)
check('station catalog retained',int(s['Station ID'].nunique()),c.execute('select count(*) from mart.dim_station').fetchone()[0])
check('pickup events not duplicated across activity rows',int(s.loc[s.Activity=='Returns','Pickups'].sum()),0)
check('return events not duplicated across activity rows',int(s.loc[s.Activity=='Pickups','Returns'].sum()),0)

weekday=c.execute("""select count(*) from range(date '2026-01-01',date '2026-11-01',interval 1 day) t(d)
 where dayofweek(d)=1""").fetchone()[0]
cell=rh[(rh.Weekday=='1 Mon')&(rh.Hour=='08:00')]
check('Monday 08:00 rate uses all calendar Mondays',round(float(cell['Trips per weekday'].sum()),8),round(float(cell.Trips.sum())/weekday,8))
check('day-normalized hourly rates reconcile',round(float(rh['Trips per calendar day'].sum())*304),expected[2026])

for end,flag in [('Pickup','start_station_mapped_to_gbfs'),('Return','end_station_mapped_to_gbfs')]:
    endpoints=frame('coverage_status');part=endpoints[endpoints.Endpoint==end]
    check(f'{end} coverage categories cover every eligible trip',int(part.Trips.sum()),expected[2026])
    actual=int(part.loc[part.Status=='Matched','Trips'].sum())
    truth=c.execute(f"select count(*) from mart.fact_trips where {flag} and started_date>='2026-01-01' and started_date<'2026-11-01'").fetchone()[0]
    check(f'{end} attribution uses departure cohort',actual,truth)

raw_counts=dict(c.execute("select started_year,count(*) from staging.stg_trips where started_year in (2025,2026) and started_month_number<=10 group by 1").fetchall())
for year,total in raw_counts.items():
    part=q[q.Year==year]
    check(f'{year} cleaning partition',int(part['Source rows'].sum()),total)
    check(f'{year} retained count',int(part.loc[part.Status=='Retained','Source rows'].sum()),expected[year])
    check(f'{year} retention shares sum to one',round(float(part.Share.sum()),12),1.0)
check('one operations row per location',len(o),int(o['Station ID'].nunique()))
check('operations count matches latest snapshot',len(o),c.execute('select count(*) from mart.fact_station_status_snapshot where snapshot_at_utc=(select max(snapshot_at_utc) from mart.fact_station_status_snapshot)').fetchone()[0])
check('no racks in pressure or shortlist',int(o.loc[o['Location type']!='Dock candidate','Pressure stations'].sum()),0)
check('no stale or inactive sites in shortlist',int(((o['Priority list']=='Yes')&(o['Fresh dock candidates']!=1)).sum()),0)
check('triage scores within 0–100',bool(o['Triage score'].between(0,100).all()),True)
check('all priority-list sites have observed pressure',bool((o.loc[o['Priority list']=='Yes','Pressure stations']==1).all()),True)

with zipfile.ZipFile(OUT/'Chicago Bikeshare Atlas.twbx') as z:
    check('package CRC',z.testzip(),None)
    wb=E.fromstring(z.read('Chicago Bikeshare Atlas.twb'))
    check('six dashboard pages',len(wb.findall('dashboards/dashboard')),6)
    check('37 editable native worksheets',len(wb.findall('worksheets/worksheet')),37)
    check('worksheet tabs hidden from viewer',all(w.get('hidden')=='true' for w in wb.findall("windows/window[@class='worksheet']")),True)
    for source in wb.findall('datasources/datasource'):
        filename=source.find('.//named-connection/connection').get('filename')
        check(f'packaged source matches local extract: {filename}',z.read('Data/'+filename),(OUT/filename).read_bytes())
        # Do not serialize CSV bytes into the audit report.
        results[-1]['actual']='matching bytes';results[-1]['expected']='matching bytes'

report={'checked_at_utc':datetime.now(timezone.utc).isoformat(),'passed_checks':len(results),
 'raw_row_growth':raw_counts[2026]/raw_counts[2025]-1,
 'eligible_trip_growth':expected[2026]/expected[2025]-1,
 'exclusion_rate_2026':1-expected[2026]/raw_counts[2026],
 'checks':results}
(OUT/'validation_report.json').write_text(json.dumps(report,indent=2))
print(f"PASS: {len(results)} dashboard reconciliation and integrity checks")
print(json.dumps({k:v for k,v in report.items() if k!='checks'},indent=2))
