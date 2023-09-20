{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
	indexes=[
      {'columns': ['id'], 'type': 'hash'},
      {'columns': ['msg_id'], 'type': 'hash'},
	  {'columns': ['to_email'], 'type': 'hash'},
	  {'columns': ['first_event_time'], 'type': 'btree'},
	  {'columns': ['last_event_time'], 'type': 'btree'},
	  {'columns': ['updated_at'], 'type': 'btree'},
    ]
	)
}}
WITH

normalize_airbyte AS(
SELECT
	{{ dbt_utils.generate_surrogate_key(['msg_id']) }} AS id,
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	opens_count,
	clicks_count,
	CAST(last_event_time AS TIMESTAMP) as last_event_time
FROM {{ source('public', 'raw_pmc_loinvites_messages') }}
{% if is_incremental() %}
WHERE CAST(last_event_time AS TIMESTAMP) >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}
)
SELECT
	id,
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	MAX(opens_count) AS opens_count,
	MAX(clicks_count) AS clicks_count,
	MIN(last_event_time) AS first_event_time,
	MAX(last_event_time) AS last_event_time,
	{{ current_date_time() }} as updated_at
FROM normalize_airbyte
GROUP BY 
	id,
	msg_id,
	status,
	subject,
	to_email,
	from_email
