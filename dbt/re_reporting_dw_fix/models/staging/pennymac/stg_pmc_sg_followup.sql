{{
  config(
    materialized = 'incremental',
    unique_key = 'msg_id',
    )
}}
WITH normalize_airbyte AS(
SELECT
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	opens_count,
	clicks_count,
	CAST(last_event_time AS TIMESTAMP) as last_event_time
FROM {{ source('public', 'pennymac_customer_profiles_messages') }} 
),
airbyte_sg AS(
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
FROM normalize_airbyte
GROUP BY msg_id,
		 status,
		 subject,
		 to_email,
		 from_email
),
freedom_sg_data AS(
SELECT	
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	MAX(opens_count) AS opens_count,
	MAX(clicks_count) AS clicks_count,
	MIN(first_event_time) AS first_event_time,
	MAX(last_event_time) AS last_event_time
FROM airbyte_sg
GROUP BY msg_id,
		 status,
		 subject,
		 to_email,
		 from_email
),
serialize_email AS(
SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY to_email ORDER BY first_event_time)  AS email_no 
FROM freedom_sg_data
)
SELECT
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	opens_count,
	clicks_count,
	email_no,
	{{ calculate_time_interval('first_event_time', '-', '5', 'hour') }} AS first_event_time,
	{{ calculate_time_interval('last_event_time', '-', '5', 'hour') }} AS last_event_time
FROM serialize_email
{% if is_incremental() %}
      WHERE last_event_time >= coalesce((select max(last_event_time) from {{ this }}), '1900-01-01')
    
  {% endif %}