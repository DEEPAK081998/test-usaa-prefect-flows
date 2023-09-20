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
  FROM {{ source('public', 'sofi_lead_submissions') }}
)

,dedup AS(
SELECT
	ga_date,
	ga_source,
	MAX(ga_users) AS users,
	MAX(ga_newusers) AS newusers,
	MAX(ga_sessions) AS sessions,
	MAX(ga_pageviews) AS pageviews,
	MAX(ga_uniquepageviews) AS unique_pageviews,
	MAX(ga_goal1completions) AS homestory_lead_submitted,
	MAX(bounces) AS bounces,
	MAX(total_secs_on_site) AS total_secs_on_page 
FROM
	added_cols
GROUP BY
	ga_date,
	ga_source
)
SELECT * FROM dedup
ORDER BY ga_date DESC