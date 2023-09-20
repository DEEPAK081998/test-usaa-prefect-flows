{{
  config(
	materialized = 'incremental',
	unique_key = 'id',
	tags=['sofi']
	)
}}

WITH

{% if is_incremental() %}
updated_cte as (
	SELECT distinct {{ dbt_utils.generate_surrogate_key(
			['ga_pagepath',
			'ga_date',
			'ga_source']
		)}} AS new_id FROM {{ source('public', 'sofi_paths_with_source') }}
	WHERE _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
),
{% endif %}

deduped AS (
	SELECT
		{{ dbt_utils.generate_surrogate_key(
			['ga_pagepath',
			'ga_date',
			'ga_source']
		)}} AS id,
		ga_pagepath,
		ga_date,
		ga_source,
		MAX(ga_uniquepageviews) as unique_pageviews,
		MAX(_airbyte_emitted_at) as updated_at
	FROM {{ source('public', 'sofi_paths_with_source') }}
	WHERE ga_pagepath LIKE '%enrollment%'
	{% if is_incremental() %}
	  and {{ dbt_utils.generate_surrogate_key(
			['ga_pagepath',
			'ga_date',
			'ga_source']
		)}} in  (SELECT new_id from updated_cte)
	{% endif %}
	GROUP BY
		ga_pagepath,
		ga_date,
		ga_source
)
SELECT
	*
FROM
	deduped
ORDER BY ga_date DESC