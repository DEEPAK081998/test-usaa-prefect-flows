{{
  config(
    materialized = 'incremental',
    unique_key = 'pup_id'
    )
}}
WITH total_lv_registrations AS(
	SELECT
	pup.id as pup_id,
	pup.created AS register_date,
	{% if target.type == 'snowflake' %}
	CAST(pup.data:agent AS TEXT) AS has_an_agent,
	CAST(pup.data:loFirstName AS TEXT) AS loFirstName,
	CAST(pup.data:loLastName AS TEXT) AS loLastName,
	CAST(pup.data:prequal AS TEXT) AS prequal,
	CAST(pup.data:sell AS TEXT) AS is_selling,
	CAST(pup.data:agentFirstName AS TEXT) AS agent_first_name,
	CAST(pup.data:agentLastName AS TEXT) AS agent_last_name,
	{% else %}
	CAST(pup.data->'agent' AS TEXT) AS has_an_agent,
	CAST(pup.data->'loFirstName' AS TEXT) AS loFirstName,
	CAST(pup.data->'loLastName' AS TEXT) AS loLastName,
	CAST(pup.data->'prequal' AS TEXT) AS prequal,
	CAST(pup.data->'sell' AS TEXT) AS is_selling,
	CAST(pup.data->'agentFirstName' AS TEXT) AS agent_first_name,
	CAST(pup.data->'agentLastName' AS TEXT) AS agent_last_name,
	{% endif %}
	partner_id,
	first_name,
	last_name,
    {{ current_date_time() }} as updated_at,
	email
	FROM {{ ref('partner_user_profiles') }} pup

	JOIN {{ ref('partner_user_roles') }} pur
	ON pup.id = pur.user_profile_id
	WHERE role = 'CUSTOMER' AND 
	partner_id = 'E2A46D0A-6544-4116-8631-F08D749045AC'
   
),

normalize_cols AS(
	SELECT
		partner_id,
		REPLACE(agent_first_name,'"','') AS agent_first_name,
		REPLACE(agent_last_name,'"','') AS agent_last_name,
		CONCAT(REPLACE(agent_first_name,'"',''), ' ', REPLACE(agent_last_name,'"','')) AS agent_name,
		pup_id,
		REPLACE(has_an_agent,'"','') AS has_agent,
		CASE WHEN is_selling = 'true' THEN 'BUY' 
			 WHEN is_selling = 'false' THEN 'SELL'
			 END AS transaction_type,
		REPLACE(prequal,'"','') AS prequal,
		REPLACE(lofirstname,'"','') AS lofirstname,
		REPLACE(lolastname,'"','') AS lolastname,
		register_date,
		first_name AS customer_first_name,
		last_name AS customer_last_name,
		CONCAT(first_name, ' ', last_name) AS customer_full_name,
		email AS customer_email,
        updated_at,
		'Freedom' AS bank_partner
	FROM 
		total_lv_registrations
     {% if is_incremental() %}
    where register_date >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}  
	
)
SELECT * FROM normalize_cols