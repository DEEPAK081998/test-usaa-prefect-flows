{{
  config(
	materialized = 'table',
	tags=['fidelity','ga']
	)
}}
WITH

aggregated_ga AS (
	SELECT
		ga_pagepath,
		ga_date,
		MAX(_airbyte_emitted_at) as airbyte_emmited,
		MAX(ga_pageviews) as pageviews,
		MAX(ga_sessions) as sessions,
		MAX(ga_avgtimeonpage) as avgtimeonpage,
		MAX(ga_bouncerate) as bouncerate,
		MAX(ga_avgsessionduration) as avgsessionduration,
		MAX(ga_pageviewspersession) as pageviewspersession,
		MAX(ga_newusers) as newusers
	FROM {{ source('public', 'fidelity_rep60_prod_ga_data') }}
	GROUP BY ga_date, ga_pagepath
)
, aggregated_users AS (
	SELECT
		ga_date,
		MAX(_airbyte_emitted_at),
		MAX(ga_users) as ga_users
	FROM {{ source('public', 'fidelity_rep60_users') }}
	GROUP BY ga_date
)
SELECT
	aggregated_ga.ga_date,
	SUM(aggregated_ga.newusers) AS newusers,
	SUM(aggregated_ga.sessions) AS sessions,
	SUM(aggregated_ga.pageviews) AS pageviews,
	MAX(aggregated_users.ga_users) AS ga_users,
	SUM((aggregated_ga.sessions*aggregated_ga.bouncerate/100)) AS bounces,
	SUM((aggregated_ga.avgsessionduration*aggregated_ga.sessions)) AS total_secs_on_site
FROM aggregated_ga
INNER JOIN aggregated_users
ON aggregated_users.ga_date = aggregated_ga.ga_date
GROUP BY aggregated_ga.ga_date
ORDER BY ga_date DESC