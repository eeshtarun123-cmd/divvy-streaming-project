"""Build compact, reconciled analytics extracts and an editable Tableau workbook.

No individual ride IDs or free-floating vehicle locations are packaged.
Run after scripts/run_pipeline.py --fetch-live.
"""
from pathlib import Path
import calendar
import copy
import csv
import json
import uuid
import zipfile
import xml.etree.ElementTree as E

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data/processed/tableau/atlas'
OUT.mkdir(parents=True, exist_ok=True)
C = duckdb.connect(str(ROOT / 'data/warehouse/divvy.duckdb'), read_only=True)
SOURCES = {}

def export(name, sql, dimensions=()):
    frame = C.execute(sql).fetchdf()
    frame.to_csv(OUT / f'{name}.csv', index=False)
    fields = {}
    for col, dtype in frame.dtypes.items():
        typ = 'integer' if dtype.kind in 'iu' else 'real' if dtype.kind == 'f' else 'string'
        if dtype.kind == 'b': typ = 'integer'; frame[col] = frame[col].astype(int)
        fields[col] = (typ, col in dimensions or typ == 'string')
    SOURCES[name] = {'fields': fields, 'frame': frame, 'calcs': {}}
    return frame

def calc(source, name, formula, fmt=None):
    SOURCES[source]['fields'][name] = ('real', False)
    SOURCES[source]['calcs'][name] = (formula, 'n'+fmt if fmt else None)

ridership = export('ridership', """
SELECT strftime(started_month,'%Y') AS Year,
       strftime(started_month,'%m %b') AS Month,
       initcap(member_casual) AS Rider,
       CASE rideable_type WHEN 'electric_bike' THEN 'E-bike' ELSE 'Classic' END AS Bike,
       initcap(member_casual) || ' / ' || CASE rideable_type WHEN 'electric_bike' THEN 'E-bike' ELSE 'Classic' END AS Segment,
       count(*)::BIGINT AS Trips,
       sum(trip_duration_minutes) AS Minutes,
       count(*)::DOUBLE / max(day(last_day(started_month))) AS "Trips per day",
       count_if(start_station_mapped_to_gbfs)::BIGINT AS "Matched pickups",
       count_if(end_station_mapped_to_gbfs)::BIGINT AS "Matched returns"
FROM mart.fact_trips
WHERE started_date >= DATE '2025-01-01' AND started_date < DATE '2026-11-01'
  AND month(started_date)<=10
GROUP BY ALL ORDER BY Year,Month,Rider,Bike
""".replace('initcap(member_casual)', "upper(left(member_casual,1)) || substr(member_casual,2)"))
calc('ridership','2026 trips','IF [Year] = "2026" THEN [Trips] ELSE 0 END', '#,##0')
calc('ridership','2025 trips','IF [Year] = "2025" THEN [Trips] ELSE 0 END', '#,##0')
calc('ridership','Additional trips','[2026 trips] - [2025 trips]', '+#,##0;-#,##0')
calc('ridership','Growth rate','SUM([2026 trips]) / SUM([2025 trips]) - 1','+0.0%;-0.0%')
calc('ridership','Pickup match rate','SUM([Matched pickups]) / SUM([Trips])','0.0%')
calc('ridership','Return match rate','SUM([Matched returns]) / SUM([Trips])','0.0%')

rhythm = export('rhythm', """
WITH days AS (SELECT dayofweek(d) AS dow,count(*) AS n FROM range(DATE '2026-01-01',DATE '2026-11-01',INTERVAL 1 DAY) t(d) GROUP BY 1),
a AS (SELECT started_day_of_week_number AS dow, started_hour,
upper(left(member_casual,1)) || substr(member_casual,2) AS Rider,
CASE rideable_type WHEN 'electric_bike' THEN 'E-bike' ELSE 'Classic' END AS Bike,
count(*)::BIGINT AS Trips, sum(trip_duration_minutes) AS Minutes
FROM mart.fact_trips WHERE started_date>=DATE '2026-01-01' AND started_date<DATE '2026-11-01' GROUP BY ALL)
SELECT CASE dow WHEN 1 THEN '1 Mon' WHEN 2 THEN '2 Tue' WHEN 3 THEN '3 Wed' WHEN 4 THEN '4 Thu' WHEN 5 THEN '5 Fri' WHEN 6 THEN '6 Sat' ELSE '7 Sun' END AS Weekday,
printf('%02d:00',started_hour) AS Hour,Rider,Bike,Trips,Minutes,
Trips::DOUBLE / n AS "Trips per weekday",Trips::DOUBLE / 304 AS "Trips per calendar day"
FROM a JOIN days USING(dow) ORDER BY Weekday,Hour,Rider,Bike
""")
calc('rhythm','Mean minutes','SUM([Minutes]) / SUM([Trips])','0.0')
calc('rhythm','Member share','SUM(IF [Rider] = "Member" THEN [Trips] ELSE 0 END) / SUM([Trips])','0.0%')
calc('rhythm','E-bike share','SUM(IF [Bike] = "E-bike" THEN [Trips] ELSE 0 END) / SUM([Trips])','0.0%')
calc('rhythm','Average daily trips','SUM([Trips]) / 304','#,##0')

duration = export('duration', """
SELECT upper(left(member_casual,1)) || substr(member_casual,2) AS Rider,
CASE WHEN trip_duration_minutes<=5 THEN '01  1–5 min' WHEN trip_duration_minutes<=10 THEN '02  5–10 min'
WHEN trip_duration_minutes<=20 THEN '03  10–20 min' WHEN trip_duration_minutes<=30 THEN '04  20–30 min'
WHEN trip_duration_minutes<=60 THEN '05  30–60 min' ELSE '06  Over 60 min' END AS "Ride length",
count(*)::BIGINT AS Trips FROM mart.fact_trips
WHERE started_date>=DATE '2026-01-01' AND started_date<DATE '2026-11-01' GROUP BY ALL ORDER BY 2,1
""")

stations = export('stations', """
WITH events AS (
 SELECT start_gbfs_station_id AS station_id,started_at_local AS at_local,1::BIGINT AS Pickups,0::BIGINT AS Returns
 FROM mart.fact_trips WHERE start_station_mapped_to_gbfs AND started_at_local>=TIMESTAMP '2026-01-01' AND started_at_local<TIMESTAMP '2026-11-01'
 UNION ALL
 SELECT end_gbfs_station_id,ended_at_local,0::BIGINT,1::BIGINT FROM mart.fact_trips
 WHERE end_station_mapped_to_gbfs AND ended_at_local>=TIMESTAMP '2026-01-01' AND ended_at_local<TIMESTAMP '2026-11-01'
), bucket AS (
 SELECT station_id,strftime(at_local,'%m %b') AS Month,printf('%02d:00',hour(at_local)) AS Hour,
 sum(Pickups)::BIGINT AS Pickups,sum(Returns)::BIGINT AS Returns FROM events GROUP BY ALL
), totals AS (SELECT station_id,sum(Pickups) p,sum(Returns) r FROM bucket GROUP BY 1),
ranks AS (SELECT *,row_number() OVER(ORDER BY r-p,station_id) loss_rank,row_number() OVER(ORDER BY r-p DESC,station_id) gain_rank FROM totals WHERE p+r>=1000)
SELECT d.station_id AS "Station ID", d.station_name AS Station,d.lat AS Latitude,d.lon AS Longitude,
d.station_capacity AS "Catalog capacity",coalesce(b.Month,'No history') AS Month,coalesce(b.Hour,'No history') AS Hour,
coalesce(b.Pickups,0)::BIGINT AS Pickups,coalesce(b.Returns,0)::BIGINT AS Returns,
(coalesce(b.Returns,0)-coalesce(b.Pickups,0))::BIGINT AS "Net flow",
(coalesce(b.Returns,0)+coalesce(b.Pickups,0))::BIGINT AS "Endpoint events",
CASE WHEN coalesce(t.p,0)+coalesce(t.r,0)=0 THEN 'No matched history'
WHEN t.r>t.p THEN 'Net arrivals' ELSE 'Net departures' END AS "Flow direction",
CASE WHEN loss_rank<=12 THEN 'Yes' ELSE 'No' END AS "Top losses",
CASE WHEN gain_rank<=12 THEN 'Yes' ELSE 'No' END AS "Top gains"
 ,CASE WHEN b.station_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS "Has history"
FROM mart.dim_station d LEFT JOIN bucket b USING(station_id) LEFT JOIN totals t USING(station_id) LEFT JOIN ranks k USING(station_id)
ORDER BY d.station_id,Month,Hour
""", dimensions=('Catalog capacity',))
# A long activity dimension gives native Tableau pickup/return lines while all
# counts and signed flow retain additive semantics in the map and KPI sheets.
pickup_rows=stations.copy();pickup_rows['Activity']='Pickups';pickup_rows['Returns']=0
pickup_rows['Endpoint events']=pickup_rows['Pickups'];pickup_rows['Net flow']=-pickup_rows['Pickups']
return_rows=stations.copy();return_rows['Activity']='Returns';return_rows['Pickups']=0
return_rows['Endpoint events']=return_rows['Returns'];return_rows['Net flow']=return_rows['Returns']
stations=pd.concat([pickup_rows,return_rows],ignore_index=True)
SOURCES['stations']['frame']=stations
SOURCES['stations']['fields']['Activity']=('string',True)
stations.to_csv(OUT/'stations.csv',index=False)
calc('stations','Pickups per day','SUM([Pickups])/304','0.0')
calc('stations','Returns per day','SUM([Returns])/304','0.0')
calc('stations','Net flow per day','SUM([Net flow])/304','+0.0;-0.0')

operations = export('operations', """
WITH latest AS (SELECT max(snapshot_at_utc) t FROM mart.fact_station_status_snapshot),
base AS (SELECT s.*,r.same_hour_start_trip_count,r.same_hour_end_trip_count,
CASE WHEN regexp_matches(lower(s.station_name),'public rack|e.station|virtual|corral') THEN 'Rack / virtual / corral'
WHEN s.station_capacity>0 THEN 'Dock candidate' ELSE 'Unknown location type' END AS location_type
FROM mart.fact_station_status_snapshot s JOIN latest ON s.snapshot_at_utc=latest.t
LEFT JOIN mart.mart_rebalancing_priority r USING(station_id)),
scored AS (SELECT *,CASE
WHEN location_type<>'Dock candidate' THEN location_type
WHEN NOT station_active_flag THEN 'Inactive'
WHEN status_last_reported_at_utc IS NULL OR date_diff('second',status_last_reported_at_utc,feed_last_updated_at_utc)>300 THEN 'Stale / unknown'
WHEN available_bikes<=2 AND available_docks<=2 THEN 'Both low'
WHEN available_bikes<=2 THEN 'Pickup pressure'
WHEN available_docks<=2 THEN 'Return pressure'
ELSE 'Available' END AS service_state FROM base)
SELECT station_id AS "Station ID",station_name AS Station,lat AS Latitude,lon AS Longitude,
location_type AS "Location type",service_state AS "Service state",available_bikes AS Bikes,
available_docks AS Docks,available_ebikes AS "E-bikes",disabled_bikes AS "Disabled bikes",
station_capacity AS Capacity,
strftime(feed_last_updated_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago','%Y-%m-%d %H:%M:%S') AS "Feed observed Chicago",
strftime(status_last_reported_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago','%Y-%m-%d %H:%M:%S') AS "Station reported Chicago",
CASE WHEN service_state='Pickup pressure' THEN 'Deliver bikes'
WHEN service_state='Return pressure' THEN 'Create return space'
WHEN service_state='Both low' THEN 'Inspect station' ELSE 'Monitor' END AS Action,
CASE WHEN service_state IN ('Pickup pressure','Return pressure','Both low') THEN 1 ELSE 0 END AS "Pressure stations",
CASE WHEN service_state IN ('Available','Pickup pressure','Return pressure','Both low') THEN 1 ELSE 0 END AS "Fresh dock candidates",
CASE WHEN service_state IN ('Available','Pickup pressure','Return pressure','Both low') AND available_docks=0 THEN 1 ELSE 0 END AS "Zero dock stations",
CASE WHEN service_state IN ('Available','Pickup pressure','Return pressure','Both low') AND available_bikes=0 THEN 1 ELSE 0 END AS "Zero bike stations",
coalesce(same_hour_start_trip_count,0) AS "Historical hour pickups",
coalesce(same_hour_end_trip_count,0) AS "Historical hour returns"
FROM scored ORDER BY Station
""")
# Rank only current, fresh, dock candidates. This is an explainable triage heuristic,
# not a forecast, truck route, service-level measure, or estimate of lost trips.
of = SOURCES['operations']['frame']
of['Triage score'] = 0.0
active = of['Fresh dock candidates'].eq(1)
live = (1 - of.loc[active, ['Bikes','Docks']].min(axis=1) / of.loc[active,'Capacity']).clip(0,1)
need = of.loc[active].apply(lambda r: r['Historical hour returns'] if r['Action']=='Create return space' else r['Historical hour pickups'], axis=1)
of.loc[active,'Triage score'] = 100*(.7*live + .3*(need/need.max() if need.max() else 0))
ranked = of[of['Pressure stations'].eq(1)].sort_values(['Triage score','Station ID'],ascending=[False,True]).head(12)
of['Priority list'] = of['Station ID'].isin(ranked['Station ID']).map({True:'Yes',False:'No'})
of.to_csv(OUT/'operations.csv',index=False)
SOURCES['operations']['fields'].update({'Triage score':('real',False),'Priority list':('string',True)})

coverage = export('coverage', """
SELECT strftime(started_month,'%Y-%m') AS Month,count(*)::BIGINT AS Trips,
count_if(start_station_mapped_to_gbfs)::BIGINT AS "Matched pickups",
count_if(end_station_mapped_to_gbfs)::BIGINT AS "Matched returns",
count_if(NOT has_start_station_id)::BIGINT AS "Missing pickup ID",
count_if(NOT has_end_station_id)::BIGINT AS "Missing return ID"
FROM mart.fact_trips WHERE started_date>=DATE '2025-01-01' AND started_date<DATE '2026-11-01' GROUP BY ALL ORDER BY 1
""")
calc('coverage','Pickup coverage','SUM([Matched pickups])/SUM([Trips])','0.0%')
calc('coverage','Return coverage','SUM([Matched returns])/SUM([Trips])','0.0%')
cov_status = export('coverage_status', """
WITH a AS (SELECT 'Pickup' AS Endpoint,
CASE WHEN start_station_mapped_to_gbfs THEN 'Matched' WHEN NOT has_start_station_id THEN 'Missing ID' ELSE 'Unmatched ID' END AS Status
FROM mart.fact_trips WHERE started_date>=DATE '2026-01-01' AND started_date<DATE '2026-11-01'
UNION ALL SELECT 'Return',CASE WHEN end_station_mapped_to_gbfs THEN 'Matched' WHEN NOT has_end_station_id THEN 'Missing ID' ELSE 'Unmatched ID' END
FROM mart.fact_trips WHERE started_date>=DATE '2026-01-01' AND started_date<DATE '2026-11-01')
SELECT Endpoint,Status,count(*)::BIGINT AS Trips FROM a GROUP BY ALL
""")

quality = export('quality', """
WITH a AS (SELECT started_year::VARCHAR AS Year,count(*)::BIGINT AS raw_count,
count_if(is_analytics_eligible)::BIGINT AS eligible_count FROM staging.stg_trips
WHERE started_year IN (2025,2026) AND started_month_number<=10 GROUP BY 1)
SELECT Year,'Retained' AS Status,eligible_count AS "Source rows",eligible_count::DOUBLE/raw_count AS Share FROM a
UNION ALL SELECT Year,'Excluded',raw_count-eligible_count,(raw_count-eligible_count)::DOUBLE/raw_count FROM a
ORDER BY 1,2
""")
calc('quality','Excluded rate','SUM(IF [Status] = "Excluded" THEN [Source rows] ELSE 0 END) / SUM([Source rows])','0.0%')

summary = {
 'history_start':'2025-01-01','history_end_exclusive':'2026-11-01',
 'comparable_months':'January–October',
 'trips_2026':int(ridership.loc[ridership.Year=='2026','Trips'].sum()),
 'trips_2025':int(ridership.loc[ridership.Year=='2025','Trips'].sum()),
 'station_catalog':int(C.execute('SELECT count(*) FROM mart.dim_station').fetchone()[0]),
 'latest_operations_locations':int(of.shape[0]),
 'operations_observed_chicago':str(of['Feed observed Chicago'].iloc[0]),
 'pipeline_health':C.execute('select * from mart.mart_pipeline_health').fetchdf().astype(str).to_dict('records')[0],
 'sources':{'trips':'https://divvybikes.com/system-data','gbfs':'https://gbfs.lyft.com/gbfs/2.3/chi/gbfs.json'},
 'extract_rows':{k:len(v['frame']) for k,v in SOURCES.items()},
}
assert summary['trips_2026']==5645807 and summary['trips_2025']==4916470
assert int(rhythm.Trips.sum())==summary['trips_2026']==int(duration.Trips.sum())
assert int(cov_status[cov_status.Endpoint=='Pickup'].Trips.sum())==summary['trips_2026']
assert int(cov_status[cov_status.Endpoint=='Return'].Trips.sum())==summary['trips_2026']
summary['additional_trips']=summary['trips_2026']-summary['trips_2025']
summary['growth_rate']=summary['additional_trips']/summary['trips_2025']
(OUT/'manifest.json').write_text(json.dumps(summary,indent=2))

def sub(parent, tag, text=None, **attrs):
    el=E.SubElement(parent,tag,{k.replace('_','-'):str(v) for k,v in attrs.items()})
    if text is not None: el.text=str(text)
    return el

WB=E.Element('workbook',{'original-version':'18.1','source-build':'2023.1.2 (20231.23.0511.1508)','source-platform':'mac','version':'18.1'})
format_manifest = sub(WB, 'document-format-change-manifest')
for feature in (
    'ObjectModelEncapsulateLegacy', 'ObjectModelTableType',
    'SchemaViewerObjectModel', 'SheetIdentifierTracking', 'SortTagCleanup',
    'WindowsPersistSimpleIdentifiers', 'MapboxVectorStylesAndLayers',
):
    sub(format_manifest, feature)
sub(WB,'preferences')
style=sub(WB,'style')
rule=sub(style,'style-rule',element='worksheet')
for attr,val in [('font-family','Tableau Book'),('font-size','10'),('color','#26384b')]:sub(rule,'format',attr=attr,value=val)
dsroot=sub(WB,'datasources')
for name,source in SOURCES.items():
    ds=sub(dsroot,'datasource',caption=name.replace('_',' ').title(),inline='true',name='federated.'+name,version='18.1')
    con=sub(ds,'connection',**{'class':'federated'})
    nc=sub(sub(con,'named-connections'),'named-connection',caption=name,name='textscan.'+name)
    sub(nc,'connection',**{'class':'textscan','directory':'Data','filename':name+'.csv'})
    rel=sub(con,'relation',connection='textscan.'+name,name=name+'.csv',table='['+name+'#csv]',type='table')
    cols=sub(rel,'columns',character_set='UTF-8',header='yes',locale='en_US',separator=',')
    mr=sub(con,'metadata-records')
    oid=name+'.csv_'+uuid.uuid5(uuid.NAMESPACE_DNS,name).hex.upper()
    for i,(field,(typ,dim)) in enumerate(source['fields'].items()):
        if field in source['calcs']:continue
        sub(cols,'column',datatype=typ,name=field,ordinal=i)
        rec=sub(mr,'metadata-record',**{'class':'column'})
        for t,txt in [('remote-name',field),('remote-type',{'integer':20,'real':5,'string':129}[typ]),('local-name',f'[{field}]'),('parent-name',f'[{name}.csv]'),('remote-alias',field),('ordinal',i),('local-type',typ),('aggregation','Count' if dim else 'Sum'),('contains-null','true'),('object-id',f'[{oid}]')]:sub(rec,t,txt)
    sub(ds,'aliases',enabled='yes')
    for field,(typ,dim) in source['fields'].items():
        col=sub(ds,'column',datatype=typ,name=f'[{field}]',role='dimension' if dim else 'measure',type='nominal' if dim else 'quantitative')
        if field in ('Latitude','Longitude'):col.set('semantic-role',f'[Geographical].[{field}]')
        if field in source['calcs']:
            formula,fmt=source['calcs'][field];sub(col,'calculation',**{'class':'tableau','formula':formula})
            if fmt:col.set('default-format',fmt)
        elif not dim:col.set('default-format','n0.0%' if field=='Share' else 'n#,##0' if typ=='integer' else 'n#,##0.0')
    for field,(typ,dim) in source['fields'].items():
        if dim:
            sub(ds,'column-instance',column=f'[{field}]',derivation='None',name=f'[none:{field}:nk]',pivot='key',type='nominal')
    sub(ds,'layout',dim_ordering='alphabetic',measure_ordering='alphabetic',show_structure='true')
    dsstyle=sub(ds,'style')
    palette_rule=sub(dsstyle,'style-rule',element='mark')
    for field,(typ,dim) in source['fields'].items():
        if dim and typ=='string':
            enc=sub(palette_rule,'encoding',attr='color',field=f'[none:{field}:nk]',type='palette')
            domain=set(source['frame'][field].drop_duplicates().astype(str))
            # Tableau resolves categorical palettes at datasource scope.
            for value,hexcolor in {'Pickups':'#087e9b','Returns':'#ef9944','2025':'#a7b6c6','2026':'#087e9b','Member':'#087e9b','Casual':'#ef9944','Classic':'#a7b6c6','E-bike':'#087e9b','Net arrivals':'#087e9b','Net departures':'#ec8754','No matched history':'#c3cad3','Available':'#2c9d8f','Pickup pressure':'#e8794c','Return pressure':'#8056a3','Both low':'#b64953','Inactive':'#a7b6c6','Stale / unknown':'#5a6878','Rack / virtual / corral':'#d0d5dc','Dock candidate':'#087e9b','Matched':'#087e9b','Missing ID':'#d5a15c','Unmatched ID':'#bd6065'}.items():
                if value in domain:
                    sub(sub(enc,'map',to=hexcolor),'bucket',json.dumps(value))
            for value,hexcolor in {'Retained':'#087e9b','Excluded':'#d5a15c'}.items():
                if value in domain:sub(sub(enc,'map',to=hexcolor),'bucket',json.dumps(value))
    obj=sub(sub(sub(ds,'object-graph'),'objects'),'object',caption=name+'.csv',id=oid)
    sub(obj,'properties',context='').append(copy.deepcopy(rel))

works=sub(WB,'worksheets'); dashboards=sub(WB,'dashboards'); windows=sub(WB,'windows')
PALETTE={'2025':'#a7b6c6','2026':'#087e9b','Member':'#087e9b','Casual':'#ef9944','Classic':'#a7b6c6','E-bike':'#087e9b','Net arrivals':'#087e9b','Net departures':'#ec8754','No matched history':'#c3cad3','Available':'#2c9d8f','Pickup pressure':'#e8794c','Return pressure':'#8056a3','Both low':'#b64953','Inactive':'#a7b6c6','Stale / unknown':'#5a6878','Rack / virtual / corral':'#d0d5dc','Dock candidate':'#087e9b','Matched':'#087e9b','Missing ID':'#d5a15c','Unmatched ID':'#bd6065'}

def sheet(name,source,rows=None,cols=None,mark='Bar',color=None,size=None,detail=(),text=None,filters=None,sort=None,title=None,mapview=False,kpi=False):
    fields=SOURCES[source]['fields'];calcs=SOURCES[source]['calcs']; dsname='federated.'+source
    w=sub(works,'worksheet',name=name)
    opt=sub(w,'layout-options');ft=sub(sub(opt,'title'),'formatted-text');sub(ft,'run',title or name,fontname='Tableau Semibold',fontsize='10' if kpi else '12',fontcolor='#26384b')
    table=sub(w,'table');view=sub(table,'view')
    sub(sub(view,'datasources'),'datasource',caption=source.replace('_',' ').title(),name=dsname)
    if mapview:sub(sub(view,'mapsources'),'mapsource',name='Tableau')
    deps=sub(view,'datasource-dependencies',datasource=dsname)
    used=[]
    for v in [rows,cols,color,size,text,*detail,*(filters or {}).keys()]:
        if v and v not in used:used.append(v)
    def agg(f):
        if fields[f][1]:return 'none'
        if f in calcs and any(x in calcs[f][0] for x in ['SUM(','AVG(','COUNT(']):return 'usr'
        if f in ['Latitude','Longitude','Triage score','Catalog capacity']:return 'avg'
        return 'sum'
    def ref(f):return f'[{dsname}].[{agg(f)}:{f}:{"nk" if fields[f][1] else "qk"}]'
    for f in used:
        typ,dim=fields[f]
        col=sub(deps,'column',datatype=typ,name=f'[{f}]',role='dimension' if dim else 'measure',type='nominal' if dim else 'quantitative')
        if f in ['Latitude','Longitude']:col.set('semantic-role',f'[Geographical].[{f}]')
        if f in calcs:
            formula,fmt=calcs[f];sub(col,'calculation',**{'class':'tableau','formula':formula})
            if fmt:col.set('default-format',fmt)
        deriv={'none':'None','sum':'Sum','avg':'Avg','usr':'User'}[agg(f)]
        sub(deps,'column-instance',column=f'[{f}]',derivation=deriv,name=f'[{agg(f)}:{f}:{"nk" if dim else "qk"}]',pivot='key',type='nominal' if dim else 'quantitative')
    for f,val in (filters or {}).items():
        fil=sub(view,'filter',**{'class':'categorical','column':ref(f)})
        sub(fil,'groupfilter',function='member',level=ref(f).split('].',1)[1],member=json.dumps(val))
    if sort and rows:sub(view,'computed-sort',column=ref(rows),direction=sort,using=ref(cols))
    if filters:
        slices=sub(view,'slices')
        for f in filters:sub(slices,'column',ref(f))
    sub(view,'aggregation',value='true')
    st=sub(table,'style')
    grid=sub(st,'style-rule',element='gridline');sub(grid,'format',attr='line-visibility',value='off')
    zero=sub(st,'style-rule',element='zeroline');sub(zero,'format',attr='line-visibility',value='on')
    if kpi:
        ar=sub(st,'style-rule',element='worksheet');sub(ar,'format',attr='background-color',value='#ffffff')
    if color and not fields[color][1]:
        cr=sub(st,'style-rule',element='mark');sub(cr,'encoding',attr='color',field=ref(color),palette='blue_teal',type='interpolated')
    pane=sub(sub(table,'panes'),'pane',selection_relaxation_option='selection-relaxation-allow')
    sub(sub(pane,'view'),'breakdown',value='auto');sub(pane,'mark',**{'class':mark})
    enc=sub(pane,'encodings')
    for tag,f in [('color',color),('size',size),('text',text)]:
        if f:sub(enc,tag,column=ref(f))
    for f in detail:sub(enc,'lod',column=ref(f))
    ps=sub(sub(pane,'style'),'style-rule',element='mark')
    for a,v in [('color','#087e9b'),('size','0.7' if mapview else '0.8'),('mark-labels-show','true' if kpi else 'false')]:sub(ps,'format',attr=a,value=v)
    if kpi:
        sub(ps,'format',attr='font-size',value='24');sub(ps,'format',attr='font-weight',value='bold')
    sub(table,'rows',ref(rows) if rows else '')
    sub(table,'cols',ref(cols) if cols else '')
    sub(w,'simple-id',uuid='{'+str(uuid.uuid4()).upper()+'}')
    win=sub(windows,'window',**{'class':'worksheet','name':name,'hidden':'true'})
    cards=sub(win,'cards');edge=sub(cards,'edge',name='top')
    sub(sub(edge,'strip',size='31'),'card',type='title')
    vp=sub(win,'viewpoint');sub(vp,'zoom',type='entire-view')
    sub(win,'simple-id',uuid='{'+str(uuid.uuid4()).upper()+'}')
    return name

def dashboard(name,title,subtitle,insight,footnote,charts,kpis=(),legends=()):
    d=sub(dashboards,'dashboard',name=name)
    sty=sub(d,'style');rr=sub(sty,'style-rule',element='dashboard');sub(rr,'format',attr='background-color',value='#f2f5f8')
    sub(d,'size',maxheight=1000,maxwidth=1440,minheight=1000,minwidth=1440)
    zones=sub(d,'zones');outer=sub(zones,'zone',h=100000,id=0,type_v2='layout-basic',w=100000,x=0,y=0)
    zid=1
    def zone(x,y,w,h,kind='text',sheetname=None,content='',font=12,bg='#ffffff',fg='#26384b'):
        nonlocal zid
        attrs={'h':int(h*100),'w':int(w/1440*100000),'x':int(x/1440*100000),'y':int(y*100),'id':zid,'is-fixed':'true'};zid+=1
        if kind=='legend':
            attrs.update({'name':sheetname,'type-v2':'color','pane-specification-id':'0','param':content})
        elif kind=='sheet':attrs.update({'name':sheetname,'show-title':'true'})
        else:attrs['type-v2']=kind
        z=sub(outer,'zone',**attrs)
        if kind=='text':
            ft=sub(z,'formatted-text')
            if '\n' in content and font>=18:
                heading,subheading=content.split('\n',1)
                sub(ft,'run',heading+'\n',fontname='Tableau Semibold',fontsize=20,fontcolor=fg)
                sub(ft,'run',subheading,fontname='Tableau Book',fontsize=12,fontcolor=fg)
            else:sub(ft,'run',content,fontname='Tableau Semibold' if font>=18 else 'Tableau Book',fontsize=font,fontcolor=fg)
        zs=sub(z,'zone-style')
        for a,v in [('background-color',bg),('border-style','none'),('margin','4'),('padding','8')]:sub(zs,'format',attr=a,value=v)
        return z
    zone(0,0,1440,110,content=title+'\n'+subtitle,font=22,bg='#122b43',fg='#ffffff')
    zone(24,122,1392,60,content=insight,font=11,bg='#e3f0f3',fg='#13546b')
    if kpis:
        width=(1392-16*(len(kpis)-1))/len(kpis)
        for i,k in enumerate(kpis):zone(24+i*(width+16),194,width,115,kind='sheet',sheetname=k)
    for sh,x,y,w,h in charts:zone(x,y,w,h,kind='sheet',sheetname=sh)
    for sh,source,field,x,y,w,h in legends:
        zone(x,y,w,h,kind='legend',sheetname=sh,content=f'[federated.{source}].[none:{field}:nk]')
    zone(24,920,1392,60,content=footnote,font=10,bg='#f2f5f8',fg='#52677b')
    win=sub(windows,'window',**{'class':'dashboard','name':name,'maximized':'true'})
    viewpoints=sub(win,'viewpoints')
    for sh, *_ in charts:sub(viewpoints,'viewpoint',name=sh).append(E.Element('zoom',{'type':'entire-view'}))
    for sh in kpis:sub(viewpoints,'viewpoint',name=sh).append(E.Element('zoom',{'type':'entire-view'}))
    sub(win, 'active', id='-1')
    sub(d,'simple-id',uuid='{'+str(uuid.uuid4()).upper()+'}')
    return d

# 01 — Executive growth story. Both periods contain the same ten months.
k1=sheet('Trips in 2026','ridership',mark='Text',text='2026 trips',kpi=True,title='2026 ELIGIBLE TRIPS  •  JAN–OCT')
k2=sheet('Trips in 2025','ridership',mark='Text',text='2025 trips',kpi=True,title='2025 COMPARABLE TRIPS  •  JAN–OCT')
k3=sheet('Year over year growth','ridership',mark='Text',text='Growth rate',kpi=True,title='YEAR OVER YEAR')
k4=sheet('Additional rides','ridership',mark='Text',text='Additional trips',kpi=True,title='ADDITIONAL TRIPS')
m=sheet('Monthly comparison','ridership',rows='Trips',cols='Month',mark='Line',color='Year',title='01  |  The same months, a larger system  •  teal 2026 / gray 2025')
g=sheet('Growth contribution','ridership',rows='Segment',cols='Additional trips',color='Bike',sort='DESC',title='02  |  Where did the additional trips come from?')
mix=sheet('Rider and bike mix','ridership',rows='Rider',cols='2026 trips',color='Bike',title='03  |  2026 usage mix  •  teal E-bike / gray Classic')
dashboard('01 Business Pulse','CHICAGO BIKESHARE  /  BUSINESS PULSE','01  Growth → riders → places → operations → evidence',
f"Full-system growth: {summary['growth_rate']:.1%}. E-bikes added 917,738 trips, offsetting a 188,401 decline in classic-bike trips. Select a segment below to explore the drivers.",
'Comparisons use Jan–Oct in both years. Eligible rides last >1 minute and <24 hours. Trips describe the Divvy service footprint; no city-limits filter is applied. Select a segment to filter the page; click blank space to reset.',
[(m,24,325,1392,300),(g,24,641,800,260),(mix,840,641,576,260)],[k1,k2,k3,k4])

# 02 — Demand rhythms with honest calendar-day denominators.
r1=sheet('Member share','rhythm',mark='Text',text='Member share',kpi=True,title='MEMBER SHARE  •  OF TRIPS')
r2=sheet('Electric share','rhythm',mark='Text',text='E-bike share',kpi=True,title='E-BIKE SHARE  •  OF TRIPS')
r3=sheet('Mean duration','rhythm',mark='Text',text='Mean minutes',kpi=True,title='MEAN TRIP LENGTH  •  MINUTES')
r4=sheet('Daily ridership','rhythm',mark='Text',text='Average daily trips',kpi=True,title='AVERAGE TRIPS PER CALENDAR DAY')
heat=sheet('Weekday hour heatmap','rhythm',rows='Weekday',cols='Hour',mark='Square',color='Trips per weekday',title='01  |  A city with a weekly rhythm  •  trips per matching calendar weekday')
hour=sheet('Rider hourly profile','rhythm',rows='Trips per calendar day',cols='Hour',mark='Line',color='Rider',title='02  |  When riders travel  •  teal Member / orange Casual')
dur=sheet('Duration distribution','duration',rows='Ride length',cols='Trips',color='Rider',title='03  |  How long do rides last?')
dashboard('02 Rider Rhythm','CHICAGO BIKESHARE  /  RIDER RHYTHM','02  Who rides, when they travel, and how long they stay',
'Read the hourly profile alongside the heatmap to distinguish weekday peaks from weekend activity. Rider type describes trips, not unique people or customer conversion.',
'Jan–Oct 2026 · 304 calendar days. Weekday heatmap divides by every matching weekday in the window, including dates with zero trips. Mean duration includes eligible rides only. Select a heatmap cell to inspect the hourly profile and cards.',
[(heat,24,325,1392,270),(hour,24,611,850,290),(dur,890,611,526,290)],[r1,r2,r3,r4])

# 03 — Station map and profiles share station keys and event-time buckets.
sm=sheet('Station flow map','stations',rows='Latitude',cols='Longitude',mark='Circle',color='Flow direction',size='Endpoint events',detail=('Station ID','Station'),mapview=True,title='01  |  Where bikes accumulate and leave  •  select a station')
sp=sheet('Station hourly profile','stations',rows='Endpoint events',cols='Hour',mark='Line',color='Activity',filters={'Has history':'Yes'},title='02  |  Local hour  •  teal Pickups / orange Returns')
st=sheet('Station monthly profile','stations',rows='Endpoint events',cols='Month',mark='Line',color='Activity',filters={'Has history':'Yes'},title='03  |  Monthly activity  •  teal Pickups / orange Returns')
sk1=sheet('Matched station pickups','stations',mark='Text',text='Pickups',kpi=True,title='MATCHED PICKUPS')
sk2=sheet('Matched station returns','stations',mark='Text',text='Returns',kpi=True,title='MATCHED RETURNS')
sk3=sheet('Matched net flow','stations',mark='Text',text='Net flow',kpi=True,title='NET FLOW  •  RETURNS MINUS PICKUPS')
dashboard('03 Station Atlas','CHICAGO BIKESHARE  /  STATION ATLAS','03  Place matters  •  geographic patterns and station drilldown',
'Teal stations receive more recorded bikes than they send out; orange stations send out more than they receive. Circle area represents matched endpoint activity. Click a station to focus the profiles and cards.',
'Jan–Oct 2026 local event time. Pickups use start timestamps; returns use end timestamps. Map uses the current catalog; some historic IDs are retired or unmatched. Capacity is a current reference, not historical capacity. Clear the map selection to reset.',
[(sm,24,325,780,576),(sp,820,325,596,276),(st,820,617,596,284)],[sk1,sk2,sk3],[(sm,'stations','Flow direction',40,760,220,120)])

# 04 — Net movements from historical trips, never claimed as observed inventory.
loss=sheet('Largest net departures','stations',rows='Station',cols='Net flow',color='Flow direction',filters={'Top losses':'Yes'},sort='ASC',title='01  |  Largest net departures  •  potential replenishment demand')
gain=sheet('Largest net arrivals','stations',rows='Station',cols='Net flow',color='Flow direction',filters={'Top gains':'Yes'},sort='DESC',title='02  |  Largest net arrivals  •  potential return-space demand')
net=sheet('Hourly signed station flow','stations',rows='Net flow per day',cols='Hour',mark='Line',filters={'Has history':'Yes'},title='03  |  Net flow by hour per calendar day  •  select a station above')
dashboard('04 Demand and Flow','CHICAGO BIKESHARE  /  DEMAND AND FLOW','04  Where historical trip patterns create a balancing challenge',
'Historical net flow = matched returns − matched pickups. Compare the largest net-loss and net-gain stations, then select one to inspect its hourly pattern. These are starting points for investigation, not truck dispatch instructions.',
'Jan–Oct 2026 · rankings require ≥1,000 matched endpoint events. Event-time buckets include zero-event calendar days in the denominator. Operator moves, station downtime, and unmatched endpoints are not observed in trip-based net flow.',
[(loss,24,205,688,380),(gain,728,205,688,380),(net,24,601,1392,300)])

# 05 — Dated operations snapshot with separate rack classification and freshness.
op1=sheet('Fresh dock count','operations',mark='Text',text='Fresh dock candidates',kpi=True,title='FRESH ACTIVE DOCK CANDIDATES')
op2=sheet('Pressure station count','operations',mark='Text',text='Pressure stations',kpi=True,title='LOW BIKE OR DOCK AVAILABILITY  •  ≤2')
op3=sheet('Zero return space','operations',mark='Text',text='Zero dock stations',kpi=True,title='ZERO AVAILABLE DOCKS')
op4=sheet('Zero available bikes','operations',mark='Text',text='Zero bike stations',kpi=True,title='ZERO AVAILABLE BIKES')
om=sheet('Operations map','operations',rows='Latitude',cols='Longitude',mark='Circle',color='Service state',detail=('Station ID','Station','Service state','Feed observed Chicago','Station reported Chicago','Bikes','Docks','Action'),mapview=True,title='01  |  Availability at the saved observation  •  hover for bikes and docks')
pr=sheet('Rebalancing shortlist','operations',rows='Station',cols='Triage score',color='Service state',filters={'Priority list':'Yes'},sort='DESC',detail=('Action','Bikes','Docks'),title='02  |  Triage shortlist  •  inspect before dispatch')
state=sheet('Service state distribution','operations',rows='Service state',cols='Bikes',color='Service state',title='03  |  Bikes by service state  •  racks shown separately')
dashboard('05 Operations Snapshot','CHICAGO BIKESHARE  /  OPERATIONS SNAPSHOT',f"05  GBFS feed observed {summary['operations_observed_chicago']} Chicago time  •  saved snapshot",
'This page is a dated observation, not an automatically refreshing live connection. Low bikes flag pickup pressure; low docks flag return pressure. Rack, virtual, and corral locations are separated from dock candidates.',
'Location type is inferred from station names and positive capacity; verify before operational use. Station reports >5 minutes older than the feed are excluded from triage. Score: 70% live scarcity + 30% relative same-hour historical demand; not a probability or forecast.',
[(om,24,325,780,576),(pr,820,325,596,345),(state,820,686,596,215)],[op1,op2,op3,op4],[(om,'operations','Service state',40,690,240,190)])

# 06 — Coverage is a first-class part of the business story.
c1=sheet('Pickup attribution','ridership',mark='Text',text='Pickup match rate',kpi=True,filters={'Year':'2026'},title='PICKUP ATTRIBUTION  •  2026')
c2=sheet('Return attribution','ridership',mark='Text',text='Return match rate',kpi=True,filters={'Year':'2026'},title='RETURN ATTRIBUTION  •  2026')
c3=sheet('Eligible comparable rides','ridership',mark='Text',text='2026 trips',kpi=True,title='CITYWIDE TRIPS RETAINED  •  2026')
cv=sheet('Attribution over time','coverage',rows='Pickup coverage',cols='Month',mark='Line',title='01  |  Pickup attribution by month  •  matched / eligible trips')
cs=sheet('Endpoint coverage breakdown','coverage_status',rows='Endpoint',cols='Trips',color='Status',title='02  |  Endpoints  •  teal Matched / gold Missing / red Unmatched')
c4=sheet('Cleaning exclusion share','quality',mark='Text',text='Excluded rate',kpi=True,filters={'Year':'2026'},title='SOURCE ROWS EXCLUDED  •  2026')
cq=sheet('Cleaning comparison','quality',rows='Year',cols='Share',color='Status',title='03  |  Cleaning  •  teal Retained / gold Excluded')
dashboard('06 Evidence and Coverage','CHICAGO BIKESHARE  /  EVIDENCE AND COVERAGE','06  Know the denominator before acting on the story',
'Growth remains similar after cleaning: source rows +14.7%, eligible trips +14.8% (Jan–Oct YoY). Station charts use matched endpoints only; missing IDs limit attribution, not citywide ridership.',
'Trip KPIs: Jan–Oct 2026. Sources: Divvy trip history + GBFS. Pipeline: 83 checks, zero errors, nine warnings (duplicates, durations, capacity metadata, unmatched IDs). Eligible rides exclude duplicates, invalid timestamps, durations ≤1 minute or ≥24 hours. No direct measures of revenue, unique riders, failed attempts, causal weather effects, or actual fees.',
[(cv,24,325,1392,270),(cs,24,611,816,290),(cq,856,611,560,290)],[c1,c2,c3,c4])

# Native Tableau selection actions; click blank space to clear the source filter.
actions=E.Element('actions')
for dash,source in [('01 Business Pulse',g),('02 Rider Rhythm',heat),('03 Station Atlas',sm),('04 Demand and Flow',loss),('04 Demand and Flow',gain),('05 Operations Snapshot',om)]:
    act=sub(actions,'action',caption='Explore '+source,name='[Action_'+uuid.uuid4().hex+']')
    sub(act,'activation',auto_clear='true',type='on-select')
    sub(act,'source',dashboard=dash,type='sheet',worksheet=source)
    cmd=sub(act,'command',command='tsc:tsl-filter');sub(cmd,'param',name='special-fields',value='all');sub(cmd,'param',name='target',value=dash)
WB.insert(list(WB).index(works),actions)
# Ensure first opening is the executive dashboard.
sub(WB,'thumbnails')
E.indent(WB)
twb=OUT/'Chicago Bikeshare Atlas.twb'
E.ElementTree(WB).write(twb,encoding='utf-8',xml_declaration=True)
package=OUT/'Chicago Bikeshare Atlas.twbx'
with zipfile.ZipFile(package,'w',zipfile.ZIP_DEFLATED) as z:
    z.write(twb,twb.name)
    for name in SOURCES:z.write(OUT/f'{name}.csv',f'Data/{name}.csv')
print(json.dumps(summary,indent=2))
print(f'Workbook: {package} ({package.stat().st_size:,} bytes); {len(works)} worksheets, {len(dashboards)} dashboards')
