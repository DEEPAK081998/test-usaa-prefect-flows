{{
  config(
	tags=['mlo_invites']
	)
}}
WITH

combine_la AS(
	SELECT
		INITCAP(CONCAT (lofirstname, ' ', lolastname)) AS la,
		email
	FROM {{ ref('dynamodb_invite_table') }}
	UNION
	SELECT
		la,
		email
	FROM {{ source('public', 'la_mapping_historical') }}
)
,la_mapping AS(
	SELECT 
		MAX(la) AS la,
		email
	FROM combine_la
	GROUP BY email
) 
 
,hist_sg AS (
	SELECT
		msg_id,
		status,
		subject,
		to_email,
		from_email,
		opens_count,
		clicks_count,
		first_event_time,
		last_event_time
	FROM {{ ref('freedom_hist_sg') }}
)

,airbyte_sg AS(
SELECT
	msg_id,
	status,
	subject,
	to_email,
	from_email,
	opens_count,
	clicks_count,
	first_event_time,
	last_event_time
FROM {{ ref('freedom_daily_sg') }}
)
,all_data as(
	SELECT * FROM hist_sg
	UNION ALL
	SELECT * FROM airbyte_sg
)
,freedom_sg_data AS(
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
FROM all_data
GROUP BY msg_id,
		 status,
		 subject,
		 to_email,
		 from_email
)
,serialize_email AS(
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
	{{ calculate_time_interval('last_event_time', '-', '5', 'hour') }} AS last_event_time,
	la_mapping.la AS inviting_la
FROM serialize_email
LEFT JOIN la_mapping ON serialize_email.to_email = la_mapping.email