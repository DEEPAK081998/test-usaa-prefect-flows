{{
  config(
	materialized = 'table',
	tags=['sofi','ga']
	)
}}
WITH
added_cols AS (
	SELECT *,
		ROUND(ga_sessions*ga_bouncerate/100) as bounces,
		ROUND(ga_avgsessionduration*ga_sessions) as total_secs_on_site
	FROM {{ source('public', 'sofi_daily_prod_ga_data') }}
)
, dedup AS (
  	SELECT
		ga_date,
		ga_pagepath,
		MAX(ga_sessions) as sessions,
		MAX(ga_newusers) as newusers,
		MAX(ga_entrances) as entrances,
		MAX(ga_uniquepageviews) as unique_pageviews,
		MAX(bounces) as bounces,
		MAX(total_secs_on_site) as total_secs_on_site,
		MAX(ga_pageviews) as pageviews
	FROM added_cols
	GROUP BY ga_date, ga_pagepath
	ORDER BY ga_date DESC
)
, agg AS(
	SELECT
		ga_date,
		SUM(newusers) as newusers,
		SUM(bounces) as bounces,
		SUM(unique_pageviews) as unique_pageviews,
		SUM(sessions) as sessions,
		SUM(pageviews) as pageviews,
		SUM(total_secs_on_site) as total_secs_on_site
	FROM dedup
	GROUP BY ga_date
	ORDER BY ga_date desc
)
,users AS(
	SELECT
		MAX(ga_users) as users,
		ga_date
	FROM {{ source('public', 'sofi_rep60_users') }}
	GROUP BY ga_date
	ORDER BY ga_date desc
)
SELECT 
	users.users as ga_users,
	agg.newusers,
	agg.bounces,
	agg.unique_pageviews,
	agg.total_secs_on_site,
	agg.pageviews,
	agg.sessions,
	agg.ga_date
FROM agg
INNER JOIN users
ON users.ga_date = agg.ga_date
ORDER BY ga_date DESC