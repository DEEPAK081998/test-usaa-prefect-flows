{{
  config(
	materialized = 'incremental',
	unique_key=['ga_date','ga_eventlabel','ga_eventaction','ga_eventcategory'],
	tags=['alliant','events']
	)
}}
WITH
 dedup AS (
	SELECT
		ga_date,
		ga_eventlabel,
		ga_eventaction,
		ga_eventcategory,
		MAX(ga_totalevents) AS ga_totalevents,
		{{ current_date_time() }} as updated_at
	FROM {{ source('public', 'alliant_ga_event') }}
	{% if is_incremental() %}
	WHERE _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
	{% endif %}
	GROUP BY 
		ga_date,
		ga_eventlabel,
		ga_eventaction,
		ga_eventcategory
)
SELECT * FROM dedup
ORDER BY ga_date DESC