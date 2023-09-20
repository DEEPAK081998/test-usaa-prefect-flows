{{
  config(
	materialized = 'table',
	tags=['alliant','sendgrid']
	)
}}
{#- 
joining historical data to airbyte data
deduplicating data
 -#}
WITH 

hist_sg AS (
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
	FROM {{ source('public', 'alliant_sendgrid') }} 
	GROUP BY msg_id,
			status,
			subject,
			to_email,
			from_email
)
,normalize_airbyte AS(
SELECT	
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	opens_count,
	clicks_count,
	CAST(last_event_time AS TIMESTAMP) as last_event_time
FROM {{ source('public', 'alliant_invites_sg_messages') }} 
)

,airbyte_sg AS(
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
)
{#- joining and depulicating joined data  -#}

,all_data as (
	SELECT * FROM hist_sg
	UNION ALL
	SELECT * FROM airbyte_sg
)

,alliant_sg_data AS(
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
FROM all_data
GROUP BY msg_id,
		 status,
		 subject,
		 to_email,
		 from_email
)
SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY to_email
		ORDER BY first_event_time
	)  AS email_no 
FROM alliant_sg_data