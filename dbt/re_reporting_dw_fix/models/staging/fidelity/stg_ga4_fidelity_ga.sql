WITH 
added_cols AS (
  SELECT *,
    TO_DATE(date, 'YYYYMMDD') as ga_date,
	ROUND(averagesessionduration*sessions) as total_secs_on_site
  FROM {{ source('public', 'ga4_fidelity_prod_ga_data') }}
)
, dedup AS (
	SELECT
		ga_date,
		sessionsourcemedium,
		sessioncampaignname,
		MAX(sessions) as sessions,
		MAX(newusers) as newusers,
		0 as entrances,
		0 as unique_pageviews,
		MAX(total_secs_on_site) as total_secs_on_site,
		MAX(screenpageviews) as pageviews
	FROM added_cols
	GROUP BY ga_date,  sessionsourcemedium, sessioncampaignname
	ORDER BY ga_date DESC
)
,agg AS(
	SELECT
		ga_date,
		sessionsourcemedium as ga_sourcemedium,
		sessioncampaignname as ga_campaign,
		SUM(newusers) as newusers,
		0 as unique_pageviews,
		SUM(sessions) as sessions,
		SUM(pageviews) as pageviews,
		SUM(total_secs_on_site) as total_secs_on_site
	FROM dedup
	GROUP BY ga_date, sessionsourcemedium, sessioncampaignname
	ORDER BY ga_date desc
)
,users AS(
	SELECT
		MAX(activeusers) as users,
		TO_DATE(date, 'YYYYMMDD') as ga_date,
		sessionsourceMedium as sourcemedium, 
		sessioncampaignName as campaignname
	FROM {{ source('public', 'ga4_fidelity_users_final') }}
	GROUP BY date, sessionsourceMedium, sessioncampaignName
	ORDER BY date desc
)
,
br as (
	select date,
	sessioncampaignname,
	sessionsourcemedium,
	sessions,
	engagedsessions,
	bouncerate,
	MAX(_airbyte_emitted_at)
	from {{ source('public', 'ga4_fidelity_bouncerate') }}
	group by 
	date,
	sessioncampaignname,
	sessionsourcemedium,
	sessions,
	engagedsessions,
	bouncerate 
)
,
bouncerate as (
    select TO_DATE(date, 'YYYYMMDD') as ga_date,
    sessioncampaignname,
    sessionsourcemedium,
	bouncerate,
sum(sessions) as sessions,
sum(engagedsessions) as engagedsessions
from br
group by 
    date,
    sessioncampaignname,
    sessionsourcemedium,
	bouncerate
)
/*,
final AS (*/
SELECT 
	users.users as ga_users,
	agg.newusers,
	agg.unique_pageviews,
	agg.total_secs_on_site,
	agg.pageviews,
	agg.ga_date,
	agg.ga_sourceMedium,
	agg.ga_campaign,
    br.sessions,
    br.engagedsessions,
    br.sessions- br.engagedsessions as bounces
FROM agg
INNER JOIN users
ON users.ga_date = agg.ga_date and users.sourceMedium = agg.ga_sourcemedium and users.campaignName = agg.ga_campaign
left join bouncerate br on br.ga_date = agg.ga_date and br.sessionsourcemedium = agg.ga_sourcemedium and br.sessioncampaignname = agg.ga_campaign
ORDER BY ga_date DESC
/*)
select ga_date,
max(bouncerate)
from final 
where ga_date = '2023-06-19'
group by ga_date*/