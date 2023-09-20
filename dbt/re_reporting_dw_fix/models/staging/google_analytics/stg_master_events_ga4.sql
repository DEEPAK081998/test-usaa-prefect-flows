with union_cte AS(
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    'lakeview' as partner 
from {{ ref('stg_ga4_lakeview_ga_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    'sofi' as partner 
from {{ ref('stg_ga4_sofi_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    'citizens' as partner 
from {{ ref('stg_ga4_citizens_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    'pennymac' as partner 
from {{ ref('stg_ga4_pmc_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    'fidelity' as partner 
from {{ ref('stg_ga4_fidelity_ga_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    'freedom' as partner 
from {{ ref('stg_ga4_freedom_events') }}
)
,
normalize_cte as (
select 
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
    ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	ga_newusers,
	updated_at,
    partner
from union_cte

)
select
 * from normalize_cte
where ga_date >= '2023-06-30'
order by ga_date desc