{{
  config(
    materialized = 'incremental',
    unique_key = 'pup_id',
    )
}}
WITH total_registrations AS (
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
    partner_id,
    first_name,
    last_name,
    email,
    CAST(pup.data:sofi.partyId::VARCHAR AS TEXT) as sofi_party_id,
    CAST(pup.data:sofi.leadId::VARCHAR AS TEXT) as sofi_lead_id,
    {% else %}
    CAST(pup.data->'agent' AS TEXT) AS has_an_agent,
    CAST(pup.data->'loFirstName' AS TEXT) AS loFirstName,
    CAST(pup.data->'loLastName' AS TEXT) AS loLastName,
    CAST(pup.data->'prequal' AS TEXT) AS prequal,
    CAST(pup.data->'sell' AS TEXT) AS is_selling,
    CAST(pup.data->'agentFirstName' AS TEXT) AS agent_first_name,
    CAST(pup.data->'agentLastName' AS TEXT) AS agent_last_name,
    partner_id,
    first_name,
    last_name,
    email,
    CAST(pup.data->'sofi'->>'partyId' AS TEXT) as sofi_party_id,
    CAST(pup.data->'sofi'->>'leadId' AS TEXT) as sofi_lead_id,
    {% endif %}
    pup.updated as updated_at
FROM {{ ref('partner_user_profiles') }} pup
JOIN {{ ref('partner_user_roles') }} pur
ON pup.id = pur.user_profile_id
WHERE role = 'CUSTOMER'
{% if is_incremental() %}
AND pup.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}
)
, normalize_cols AS(
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
        sofi_party_id,
        sofi_lead_id,
        updated_at
    FROM total_registrations
)
SELECT * FROM normalize_cols
