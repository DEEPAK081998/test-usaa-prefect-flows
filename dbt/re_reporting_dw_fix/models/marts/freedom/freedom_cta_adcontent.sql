{{
  config(
	materialized = 'table',
    tags=['freedom','ga','cta','adcontent']
	)
}}
{#- bounce rate and time on page -#}
WITH bounce_time AS(
SELECT 
	*,
	ROUND(ga_sessions*ga_bouncerate/100) as bounces,
	ROUND(ga_avgtimeonpage*ga_sessions) as total_secs_on_page
FROM {{ source('public', 'freedom_cta') }}
)
SELECT 
	ga_date,
	ga_medium,
	ga_source,
	ga_campaign,
	ga_adcontent,
	MAX(ga_users) AS users,
	MAX(ga_newusers) AS newusers,
	MAX(ga_sessions) AS sessions,
	MAX(bounces) as bounces,
	MAX(total_secs_on_page) AS total_secs_on_page,
	MAX(ga_pageviews) AS pageviews
FROM bounce_time
GROUP BY
	ga_date,
	ga_medium,
	ga_source,
	ga_campaign,
	ga_adcontent
ORDER BY ga_date DESC