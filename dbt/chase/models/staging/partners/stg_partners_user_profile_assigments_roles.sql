{{
  config(
	materialized = 'incremental',
	unique_key = ['lead_id','aggregate_id'],
	enabled=false
	)
}}
SELECT
	ca.lead_id,
	pup.aggregate_id,
	concat(pup.first_name,' ',pup.last_name) as name, 
	pup.email,
	pup.phones,
	pup.phones::json->0->'phoneNumber' as phoneNumber,
	ca.role,
	pup.updated as partner_updated_at,
	ca.created as current_assigment_created_at 
FROM {{ ref('partner_user_profiles') }}   AS pup 
JOIN {{ ref('current_assignments') }}  ca ON profile_aggregate_id=aggregate_id 
{% if is_incremental() %}
WHERE pup.updated >= coalesce((select max(partner_updated_at) from {{ this }}), '1900-01-01')
OR ca.created>= coalesce((select max(current_assigment_created_at) from {{ this }}), '1900-01-01')
{% endif %}