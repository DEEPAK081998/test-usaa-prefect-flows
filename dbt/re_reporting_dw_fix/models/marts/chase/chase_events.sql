{{
  config(
	materialized = 'table',
	tags=['chase','ga']
	)
}}
WITH dedup AS(
SELECT
	ga_date,
	ga_eventlabel,
	ga_eventaction,
	MAX(ga_totalevents) AS ga_totalevents,
	ga_eventcategory
FROM {{ source('public', 'chase_ga_event') }}
GROUP BY
	ga_date,
	ga_eventlabel,
	ga_eventaction,
	ga_eventcategory
)
SELECT * FROM dedup
ORDER BY ga_date DESC