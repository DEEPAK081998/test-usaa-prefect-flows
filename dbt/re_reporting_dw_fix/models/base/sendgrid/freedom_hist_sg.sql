{{
  config(
    materialized = 'incremental',
    unique_key = 'msg_id',
    )
}}
SELECT
		msg_id,
		status,
		subject,
		to_email,
		from_email,
		MAX(opens_count) AS opens_count,
		MAX(clicks_count) AS clicks_count,
		MIN(last_event_time) AS first_event_time,
		MAX(last_event_time) AS last_event_time
FROM {{ source('public', 'freedom_sendgrid') }}
{% if is_incremental() %}
WHERE last_event_time > coalesce((select max(last_event_time) from {{ this }}), '1900-01-01')
{% endif %}
GROUP BY
    msg_id,
	status,
	subject,
	to_email,
	from_email