{{
  config(
    materialized = 'incremental',
    unique_key = ['lead_id','bank_id'],
    )
}}
WITH

agent_data_cte AS (
	SELECT * FROM {{ ref('stg_partners_user_profile_assigments_roles') }}
	WHERE role='AGENT'

)

, la_data_cte AS (
	SELECT * FROM {{ ref('stg_partners_user_profile_assigments_roles') }}
	WHERE role='MLO'
)

, final_cte AS (
	SELECT 
		id as lead_id,
        leads.bank_id as bank_id,
		concat(first_name,' ',last_name) as customer_name, 
		leads.email as customer_email, 
		leads.phone as customer_phone, 
		created as enrolled_date,
		agent_data.name as agent_name,
		agent_data.email as agent_email,
		agent_data.phoneNumber as agent_phone,
		agent_data.phoneType as agent_phone_type,
		la_data.name as la_name,
		la_data.email as la_email,
		la_data.phoneNumber as la_phone,
		la_data.phoneType as la_phone_type,
        leads.updated as updated_at,
        leads.created as created_at
	FROM {{ ref('leads') }} leads
	LEFT JOIN agent_data_cte agent_data ON agent_data.lead_id=leads.id
	LEFT JOIN la_data_cte la_data ON la_data.lead_id=leads.id
    {% if is_incremental() %}
    WHERE leads.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)
SELECT * FROM final_cte