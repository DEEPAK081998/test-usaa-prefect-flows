{{
  config(
	materialized = 'incremental',
	unique_key=['ga_date','ga_eventlabel','ga_eventaction','ga_eventcategory','ga_source','ga_adcontent'],
	tags=['pmc','events']
	)
}}
WITH 
dedup AS(
	SELECT
		ga_date,
		ga_eventlabel,
		ga_eventaction,
		ga_eventcategory,
		ga_source,
		ga_adcontent,
		MAX(ga_totalevents) AS ga_totalevents,
		MAX(ga_uniqueevents) as ga_uniqueevents,
		MAX(ga_bouncerate) AS ga_bouncerate,
		MAX(ga_sessions) AS ga_sessions,
		MAX(ga_avgtimeonpage) AS ga_avgtimeonpage,
		MAX(ga_newusers) AS ga_newusers,
		{{ current_date_time() }} as updated_at
	FROM {{ source('public', 'pennymac_event_totals') }}
	{% if is_incremental() %}
	WHERE _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
	{% endif %}
	GROUP BY
		ga_date,
		ga_eventlabel,
		ga_eventaction,
		ga_eventcategory,
		ga_source,
		ga_adcontent
)
SELECT * FROM dedup
ORDER BY ga_date DESC