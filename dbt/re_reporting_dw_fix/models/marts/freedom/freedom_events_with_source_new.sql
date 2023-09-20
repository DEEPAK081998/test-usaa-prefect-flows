{{
  config(
    materialized = 'table',
	tags=['freedom','ga','events']
  )
}}
WITH dedup AS(
SELECT 
	ga_date,
	ga_source,
	ga_eventlabel,
	ga_eventaction,
	ga_eventcategory,
	MAX(ga_totalevents) AS total_events,
	MAX(ga_uniqueevents) AS unique_events
FROM {{ source('public', 'freedom_event_totals') }}
GROUP BY
	ga_date,
	ga_source,
	ga_eventlabel,
	ga_eventaction,
	ga_source,
	ga_eventcategory
)
SELECT 
	*
FROM 
	dedup