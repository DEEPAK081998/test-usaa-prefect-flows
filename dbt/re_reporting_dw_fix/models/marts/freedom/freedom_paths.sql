{{
  config(
	materialized = 'table',
	tags=['freedom','ga','paths']
	)
}}
WITH added_cols AS (
  SELECT *,
	ROUND(ga_sessions*ga_bouncerate/100) as bounces,
	ROUND(ga_avgsessionduration*ga_sessions) as total_secs_on_site
  FROM {{ source('public', 'freedom_rep60_prod_ga_data') }}
)
,dedup AS (
  SELECT
	ga_date,
	ga_pagepath,
	MAX(ga_users) as users,
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
SELECT * FROM dedup