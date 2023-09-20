{{
  config(
    materialized = 'incremental',
    unique_key = 'msg_id',
    )
}}
-- normalize
WITH normal AS(
	SELECT
		msg_id,
		status,
		subject,
		to_email,
		opens_count,
		clicks_count,
		CAST(last_event_time AS TIMESTAMP) as last_event_time
	FROM {{source('public', 'kitsap_customer_profiles_messages')}}
),

dedup AS(
	SELECT 
		msg_id,
		status,
		subject,
		to_email,
		MAX(opens_count) AS opens_count,
		MAX(clicks_count) AS clicks_count,
		MIN(last_event_time) AS first_event_time,
		MAX(last_event_time) AS last_event_time
	FROM normal
	GROUP BY
		msg_id,
		status,
		subject,
		to_email
),
-- serialize emails sent and localize time emails sent to CTZ
serialize_email AS(
	SELECT
		msg_id,
		status,
		subject,
		to_email,
		opens_count,
		clicks_count,
		{{ calculate_time_interval('first_event_time', '-', '5', 'hour') }} AS first_event_time,
		{{ calculate_time_interval('last_event_time', '-', '5', 'hour') }} AS last_event_time,
		ROW_NUMBER() OVER(PARTITION BY to_email
						  ORDER BY first_event_time) AS email_no
	FROM dedup
)
SELECT 
	*,
	CASE WHEN opens_count > 0 THEN True ELSE False END AS unique_open,
	CASE WHEN clicks_count > 0 THEN True ELSE False END AS unique_click
FROM serialize_email
  {% if is_incremental() %}
    where last_event_time >= coalesce((select max(last_event_time) from {{ this }}), '1900-01-01')
  {% endif %}
