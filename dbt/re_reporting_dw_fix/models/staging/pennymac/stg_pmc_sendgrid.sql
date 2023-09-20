{{
  config(
    materialized = 'view',
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
	FROM {{ source('public', 'pennymac_customer_profiles_messages') }}
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

SELECT * FROM serialize_email