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
    ],
	enabled=false
	)
}}
WITH

normalize_airbyte AS(
SELECT
	{{ dbt_utils.surrogate_key(['msg_id']) }} AS id,
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	opens_count,
	clicks_count,
	CAST(last_event_time AS TIMESTAMP)
FROM {{ source('public', 'freedom_daily_sg_messages') }}
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
	NOW() as updated_at
FROM normalize_airbyte
GROUP BY 
	id,
	msg_id,
	status,
	subject,
	to_email,
	from_email
