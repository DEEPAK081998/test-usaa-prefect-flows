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
	cast(ga_bouncerate as decimal) as ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	cast(ga_newusers as int) as ga_newusers,
	updated_at,
    'lakeview' as partner 
from {{ ref('stg_lakeview_ga_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    cast(null as text) as ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	cast(null as int) as ga_bouncerate,
	cast(null as int) as ga_sessions,
	cast(null as int) as ga_avgtimeonpage,
	cast(null as int) as ga_newusers,
	{{ current_date_time() }} as updated_at,
    'sofi' as partner 
from {{ ref('stg_sofi_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    cast(null as text) as ga_source,
    ga_adcontent,
    ga_totalevents,
	ga_uniqueevents,
	cast(null as int) as ga_bouncerate,
	cast(null as int) as ga_sessions,
	cast(null as int) as ga_avgtimeonpage,
	cast(null as int) as ga_newusers,
	{{ current_date_time() }} as updated_at,
    'citizens' as partner 
from {{ ref('stg_citizens_events') }}
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
	cast(ga_bouncerate as decimal) as ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	cast(ga_newusers as int) as ga_newusers,
	updated_at,
    'pennymac' as partner 
from {{ ref('stg_pmc_events') }}
union all 
select
    ga_date,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    null as ga_source,
    null as ga_adcontent,
    ga_totalevents,
	ga_totalevents as ga_uniqueevents,
	0 as ga_bouncerate,
	0 as ga_sessions,
	0 as ga_avgtimeonpage,
	0 as ga_newusers,
	updated_at,
    'fidelity' as partner 
from {{ ref('stg_fidelity_events') }}
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
	cast(ga_bouncerate as decimal) as ga_bouncerate,
	ga_sessions,
	ga_avgtimeonpage,
	cast(ga_newusers as int) as ga_newusers,
	updated_at,
    'freedom' as partner 
from {{ ref('stg_freedom_events') }}
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
where ga_date < '2023-06-30'
order by ga_date desc