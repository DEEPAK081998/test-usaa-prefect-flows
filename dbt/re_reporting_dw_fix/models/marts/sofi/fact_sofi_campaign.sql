{{
  config(
	materialized = 'incremental',
	unique_key = ['id'],
	tags=['sofi']
	)
}}
WITH sofi_bounces AS(
SELECT *,
	ROUND(ga_sessions*ga_bouncerate/100) AS bounces,
	ROUND(ga_avgtimeonpage*ga_sessions) AS total_secs_on_site
FROM {{ source('public', 'sofi_campaign') }}
),

-- DEDUP
dedup AS(
SELECT
	ga_date,
	ga_medium,
	ga_source,
	ga_campaign,
	MAX(ga_users) AS max_ga_users,
	MAX(ga_newusers) AS newusers,
	MAX(ga_sessions) AS sessions,
	MAX(ga_adcontent) AS adcontent,
	MAX(ga_pageviews) AS pageviews,
	MAX(bounces) AS bounces,
	MAX(total_secs_on_site) AS total_secs_on_site,
	MAX(ga_uniquepageviews) AS uniquepageviews
FROM sofi_bounces
GROUP BY
	ga_date,
	ga_medium,
	ga_source,
	ga_campaign
ORDER BY ga_date DESC
	)
SELECT
	{{ dbt_utils.generate_surrogate_key(
        ['ga_date',
        'ga_medium',
        'ga_source',
        'ga_campaign']
    )}} AS id,
	*
FROM dedup