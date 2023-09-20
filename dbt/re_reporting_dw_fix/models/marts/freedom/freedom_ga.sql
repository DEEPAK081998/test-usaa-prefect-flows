{{
  config(
	materialized = 'table',
	tags=['freedom','ga']
	)
}}
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
        partner,
    case when ga_date >= '2023-06-27' then 1-(sum(engagedsessions)/sum(sessions)) else sum(bounces)/sum(sessions) end as bounce_rate
from {{ ref('stg_master_ga_union') }}
where partner = 'freedom'
group by ga_users,
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