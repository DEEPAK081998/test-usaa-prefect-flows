{{
  config(
	materialized = 'incremental',
	unique_key=['id','registered_email','lead_id'],
	tags=['freedom','dates']
	)
}}
WITH

enrollments_cte AS (
	SELECT
		lead_id,
		customer_name,
		customer_email,
		customer_phone,
		enrolled_date,
		agent_name,
		agent_email,
		agent_phone,
		agent_phone_type,
		la_name,
		la_email,
		la_phone,
		la_phone_type
	FROM {{ ref('stg_enrollments') }}
	WHERE bank_id='3405DC7C-E972-4BC4-A3DA-CB07E822B7C6' AND created_at > '2021-09-07'::date
)

, final_cte AS (
	SELECT
		pup.id as id,
		pup.email as registered_email,
		pup.created as register_date,
		pup.updated as updated_at,
		enrollments.*
	FROM {{ ref('partner_user_profiles') }} as pup 
	JOIN {{ ref('partner_user_roles') }} ON pup.id=partner_user_roles.user_profile_id
	LEFT OUTER JOIN enrollments_cte enrollments ON enrollments.customer_email = pup.email
	WHERE partner_id='3405DC7C-E972-4BC4-A3DA-CB07E822B7C6'
	AND role='CUSTOMER'
	{% if is_incremental() %}
	AND pup.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
	{% endif %}
)
SELECT * FROM final_cte