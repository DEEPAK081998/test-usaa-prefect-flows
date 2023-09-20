{{
  config(
	tags=['mlo_invites']
	)
}}
-- dedup mappings
WITH dedup_dynamo AS(
	SELECT
		email,
		lofirstname,
		lolastname,
		MAX(firstname) AS firstname,
		lastname,
		phonenumber,
		lophonenumber,
		loemail
	FROM {{ ref('dynamodb_invite_table') }}
	GROUP BY
		email,
		lofirstname,
		lolastname,
		phonenumber,
		lophonenumber,
		loemail,
		firstname,
		lastname
)
-- add in what we have for historical data
, combined_la_data AS(
	SELECT
		firstname AS lo_first_name,
		lastname AS lo_last_name_,
		email AS client_email
	FROM dedup_dynamo
	UNION
	SELECT 
		split_part(la,' ','1') AS lo_first_name,
		split_part(la,' ','2') AS lo_last_name_,
		email AS client_email
	FROM {{ source('public', 'la_mapping_historical') }}
)
-- join this data to what we have already in dynamo to get the rest of data

, all_dynamo_data AS(

	SELECT
		COALESCE(dedup_dynamo.lofirstname, combined_la_data.lo_first_name) AS lo_first_name,
		COALESCE(dedup_dynamo.lolastname, combined_la_data.lo_last_name_)AS lo_last_name,
		firstname AS client_first_name,
		lastname AS client_last_name,
		COALESCE(dedup_dynamo.email, combined_la_data.client_email)	 AS client_email,
		phonenumber AS client_phone,
		lophonenumber AS lo_phone,
		loemail AS lo_email
	FROM dedup_dynamo
	FULL JOIN combined_la_data
	ON dedup_dynamo.email = combined_la_data.client_email	
)

-- dedup
, dedup_all_dynamo AS(
	SELECT 
		lo_first_name,
		lo_last_name,
		MAX(client_first_name) AS client_first_name,
		client_last_name,
		client_email,
		client_phone,
		lo_phone,
		lo_email
	FROM all_dynamo_data
	GROUP BY
		lo_first_name,
		lo_last_name,
		client_last_name,
		client_email,
		client_phone,
		lo_phone,
		lo_email
)

-- joining historical data to airbyte data
-- deduplicating data
, hist_sg AS (
	SELECT 
		to_email,
		MIN(first_event_time) AS first_event_time
	FROM {{ ref('freedom_hist_sg') }}
	GROUP BY to_email
)
, airbyte_sg AS(
	SELECT
		to_email,
		MIN(last_event_time) AS last_event_time
	FROM {{ ref('freedom_daily_sg') }}
	GROUP BY to_email
)
-- joining and depulicating joined data
, all_sg_data as(
	SELECT * FROM hist_sg
	UNION ALL
	SELECT * FROM airbyte_sg
)

, freedom_sg_data AS (
	SELECT
		to_email,
		MIN(first_event_time) AS first_event_time
	FROM all_sg_data
	GROUP BY
		to_email
)
, final AS(
	SELECT
		to_email,
		{{ calculate_time_interval('first_event_time', '-', '5', 'hour') }} AS enrolleddate,
		dedup_all_dynamo.*
	FROM freedom_sg_data
	LEFT JOIN dedup_all_dynamo ON freedom_sg_data.to_email = dedup_all_dynamo.client_email
)
, freedom_rec_invites AS(
	SELECT
		'TOF' AS program,
		'Freedom' AS client,
		NULL AS id,
		enrolleddate,
		client_first_name,
		client_last_name,
		client_email,
		client_phone,
		NULL AS purchase_location,
		NULL AS normalizedcity,
		NULL AS normalizedstate,
		NULL AS normalizedzip,
		NULL AS agentfirstname,
		NULL AS agentlastname,
		NULL AS agent_phone,
		NULL AS agent_email,
		NULL AS enrollmenttype,
		lo_first_name,
		lo_last_name,
		lo_phone,
		lo_email
	FROM
		final
)
SELECT * FROM freedom_rec_invites
ORDER BY enrolleddate DESC