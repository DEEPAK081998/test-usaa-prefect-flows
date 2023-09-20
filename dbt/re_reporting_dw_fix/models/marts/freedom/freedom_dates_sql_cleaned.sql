{{
  config(
	materialized = 'table',
	tags=['freedom','dates']
	)
}}
WITH json_unpack AS(
SELECT 
	agent_email,
	agent_name,
	CAST(agent_phone_type AS VARCHAR) AS agent_phonetype,
	CAST(agent_phone AS VARCHAR) AS agent_phonenumber,
	customer_email,
	customer_name,
	customer_phone,
	enrolled_date,
	la_email,
	la_name,
	lead_id,
	register_date,
	CAST(la_phone_type AS VARCHAR) AS la_phonetype,
	CAST(la_phone AS VARCHAR) AS la_phonenumber
FROM {{ ref('freedom_dates') }}
)
SELECT
	agent_email,
	agent_name,
	REPLACE(agent_phonetype, '"','') AS agent_phonetype,
	REPLACE(agent_phonenumber, '"','') AS agent_phonenumber,
	customer_email,
	customer_name,
	customer_phone,
	enrolled_date,
	la_email,
	la_name,
	lead_id,
	register_date,
	REPLACE(la_phonetype, '"','') AS la_phonetype,
	REPLACE(la_phonenumber, '"','') AS la_phonenumber
FROM 
	json_unpack