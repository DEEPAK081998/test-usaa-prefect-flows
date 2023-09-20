
with rbc_ga_unbounce_rec as (
	select 
		ga_date as date,
		0 as invite_count,
		0 as unique_opens,
		0 as unique_clicks,
		sum(ga_newusers) as unique_traffic_unbounce,
		0 as unique_traffic_rec,
		0 as enrollment_count_unbounce,
		0 as enrollment_count_rec
	from {{ source('public', 'unbounce_ga_metrics') }}
	where ga_landingpagepath like '%rbc%' 
		and  to_char(ga_date, 'yyyymmdd') < '20230701'
	group by ga_date
)
, rbc_ga4_unbounce_rec as (
	select 
		TO_DATE(date, 'YYYYMMDD') as date,
		0 as invite_count,
		0 as unique_opens,
		0 as unique_clicks,
		sum(newusers) as unique_traffic_unbounce,
		0 as unique_traffic_rec,
		0 as enrollment_count_unbounce,
		0 as enrollment_count_rec
	from {{ source('public', 'unbounce_ga4_metrics') }}
	where pagepath like '%rbc%' 
		and  date >= '20230701'
	group by TO_DATE(date, 'YYYYMMDD') 
	union 
	select 
		TO_DATE(date, 'YYYYMMDD')  as date,
		0 as invite_count,
		0 as unique_opens,
		0 as unique_clicks,
		0 as unique_traffic_unbounce,
		SUM(CASE WHEN lower(sessionsourcemedium) LIKE '%email%' THEN newusers ELSE 0 END) AS unique_traffic_rec,
		sum(CASE WHEN eventname ilike '%enrollment%' AND lower(sessionsourcemedium) like '%unbounce%' THEN conversions ELSE 0 END) as enrollment_count_unbounce,
		sum(CASE WHEN eventname ilike '%enrollment%' AND  lower(sessionsourcemedium) like '%email%' THEN conversions ELSE 0 END) as enrollment_count_rec
	from {{ source('public', 'rbc_ga4_data_unbounce_v2') }}
	where date >= '20230701'
	group by TO_DATE(date, 'YYYYMMDD') 
)
	
, stg_ga4_rbc_ga_unbounce as (
select 
	date, invite_count, unique_opens, unique_clicks, unique_traffic_unbounce, unique_traffic_rec, enrollment_count_unbounce, enrollment_count_rec
from rbc_ga_unbounce_rec
union
select 
	date, invite_count, unique_opens, unique_clicks, unique_traffic_unbounce, unique_traffic_rec, enrollment_count_unbounce, enrollment_count_rec
from rbc_ga4_unbounce_rec)

select 
	date,
	invite_count,
	unique_opens,
	unique_clicks,
	unique_traffic_unbounce,
	COALESCE(unique_traffic_rec, 0) unique_traffic_rec,
	enrollment_count_unbounce,
	enrollment_count_rec
from stg_ga4_rbc_ga_unbounce
