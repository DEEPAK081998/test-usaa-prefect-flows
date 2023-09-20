{{
  config(
	materialized = 'incremental',
	unique_key=['ga_date','ga_eventlabel','ga_eventaction','ga_eventcategory','ga_source'],
	tags=['lakeview','events']
	)
}}
WITH
dedup AS(
	SELECT
		TO_DATE(date, 'YYYYMMDD') as ga_date,
		"customEvent:label" as ga_eventlabel,
		"customEvent:action" as ga_eventaction,
		eventName as ga_eventcategory,
		sessionsource as ga_source,
		--'' as ga_adcontent,
		MAX(eventCount) AS ga_totalevents,
		0 as ga_uniqueevents,
		MAX(bouncerate) AS ga_bouncerate,
		MAX(sessions) AS ga_sessions,
		0 AS ga_avgtimeonpage,
		MAX(activeusers) AS ga_newusers,
		{{ current_date_time() }} as updated_at
	FROM {{ source('public', 'ga4_lakeview_event_totals') }}
	{% if is_incremental() %}
	WHERE _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
	{% endif %}
	GROUP BY
		date,
		"customEvent:label",
		"customEvent:action",
		eventName,
		sessionsource
		--ga_adcontent
)
SELECT * FROM dedup
ORDER BY ga_date DESC