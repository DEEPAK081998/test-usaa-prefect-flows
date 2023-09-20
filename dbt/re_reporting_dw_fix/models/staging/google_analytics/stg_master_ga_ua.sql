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
	0 as engagedsessions,
    'lakeview' as partner
from
{{ ref('stg_lakeview_ga') }}
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
	0 as engagedsessions,
    'sofi' as partner 
from 
{{ ref('stg_sofi_ga') }}
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
	0 as engagedsessions,
    'citizens' as partner 
from
{{ ref('stg_citizens_ga') }}
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
	0 as engagedsessions,
    'pennymac' as partner 
from
{{ ref('stg_pmc_ga') }}
union all
select 
    ga_users,
	newusers,
	bounces,
	0 as unique_pageviews,
	total_secs_on_site,
	pageviews,
	sessions,
	ga_date,
    null as ga_sourceMedium,
	null as ga_campaign,
	0 as engagedsessions,
    'fidelity' as partner 
from
{{ ref('stg_fidelity_ga') }}
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
	0 as engagedsessions,
    'freedom' as partner 
from
{{ ref('stg_freedom_ga') }}
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
where ga_date < '2023-06-27'
order by ga_date desc 