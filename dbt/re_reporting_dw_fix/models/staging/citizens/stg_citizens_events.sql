{{
  config(
	materialized = 'table',
	tags=['citizens','ga']
	)
}}
WITH dedup AS(
SELECT
	ga_date,
	ga_eventlabel,
	ga_eventaction,
	ga_eventcategory,
	ga_adcontent,
	MAX(ga_totalevents) AS ga_totalevents,
	MAX(ga_uniqueevents) as ga_uniqueevents
FROM {{ source('public', 'citizens_events_final') }}
GROUP BY
	ga_date,
	ga_eventlabel,
	ga_eventaction,
	ga_eventcategory,
	ga_adcontent
)
SELECT * FROM dedup
ORDER BY ga_date DESC