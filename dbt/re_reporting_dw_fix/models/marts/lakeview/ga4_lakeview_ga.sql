{{
  config(
	materialized = 'table',
	tags=['lakeview','ga']
	)
}}
WITH 
added_cols AS (
  SELECT *,
    TO_DATE(date, 'YYYYMMDD') as ga_date,
	ROUND(sessions*bouncerate) as bounces,
	ROUND(averagesessionduration*sessions) as total_secs_on_site
  FROM {{ source('public', 'ga4_lakeview_prod_ga_data') }}
)
, dedup AS (
	SELECT
		ga_date,
		sessionsourcemedium,
		sessioncampaignname,
		pagepath,
		MAX(sessions) as sessions,
		MAX(newusers) as newusers,
		0 as entrances,
		0 as unique_pageviews,
		MAX(bounces) as bounces,
		MAX(total_secs_on_site) as total_secs_on_site,
		MAX(screenpageviews) as pageviews
	FROM added_cols
	GROUP BY ga_date, pagepath, sessionsourcemedium, sessioncampaignname
	ORDER BY ga_date DESC
)
,agg AS(
	SELECT
		ga_date,
		sessionsourcemedium as ga_sourcemedium,
		sessioncampaignname as ga_campaign,
		SUM(newusers) as newusers,
		SUM(bounces) as bounces,
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
	FROM {{ source('public', 'ga4_lakeview_users_final_v2') }}
	GROUP BY date, sessionsourceMedium, sessioncampaignName
	ORDER BY date desc
)
SELECT 
	users.users as ga_users,
	agg.newusers,
	agg.bounces,
	agg.unique_pageviews,
	agg.total_secs_on_site,
	agg.pageviews,
	agg.sessions,
	agg.ga_date,
	agg.ga_sourceMedium,
	agg.ga_campaign
FROM agg
INNER JOIN users
ON users.ga_date = agg.ga_date and users.sourceMedium = agg.ga_sourcemedium and users.campaignName = agg.ga_campaign
ORDER BY ga_date DESC