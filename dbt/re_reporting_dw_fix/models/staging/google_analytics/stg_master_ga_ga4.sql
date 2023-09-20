with union_cte AS(
select
    ga_users,
	newusers,
	bounces,
	unique_pageviews,
	total_secs_on_site,
	pageviews,
	sessions,
	ga_date,
	ga_sourceMedium,
	ga_campaign,
	engagedsessions,
    'lakeview' as partner
from
{{ ref('stg_ga4_lakeview_ga') }}
union all
select 
    ga_users,
	newusers,
	bounces,
	unique_pageviews,
	total_secs_on_site,
    pageviews,
	sessions,
	ga_date,
    null as ga_sourceMedium,
	null as ga_campaign,
	engagedsessions,
    'sofi' as partner 
from 
{{ ref('stg_ga4_sofi_ga') }}
UNION all
select 
    ga_users,
	newusers,
	bounces,
	unique_pageviews,
	total_secs_on_site,
	pageviews,
	sessions,
	ga_date,
    null as ga_sourceMedium,
	null as ga_campaign,
	engagedsessions,
    'citizens' as partner 
from
{{ ref('stg_ga4_citizens_ga') }}
union all 
select 
    ga_users,
	newusers,
	bounces,
	unique_pageviews,
	total_secs_on_site,
	pageviews,
	sessions,
	ga_date,
    null as ga_sourceMedium,
	null as ga_campaign,
	engagedsessions,
    'pennymac' as partner 
from
{{ ref('stg_ga4_pmc_ga') }}
union all 
select 
    ga_users,
	newusers,
	bounces,
	unique_pageviews,
	total_secs_on_site,
	pageviews,
	sessions,
	ga_date,
    null as ga_sourceMedium,
	null as ga_campaign,
	engagedsessions,
    'fidelity' as partner 
from
{{ ref('stg_ga4_fidelity_ga') }}
union all 
select 
    ga_users,
	newusers,
	bounces,
	unique_pageviews,
	total_secs_on_site,
	pageviews,
	sessions,
	ga_date,
    null as ga_sourceMedium,
	null as ga_campaign,
	engagedsessions,
    'freedom' as partner 
from
{{ ref('stg_ga4_freedom_ga') }}
)
,
normalize_cte AS(
	select 
        ga_users,
        newusers,
        bounces,
        unique_pageviews,
        total_secs_on_site,
        pageviews,
        sessions,
        ga_date,
        ga_sourceMedium,
        ga_campaign,
		engagedsessions,
        partner
    from union_cte
)
select * from normalize_cte
where ga_date >= '2023-06-27'
order by ga_date desc 