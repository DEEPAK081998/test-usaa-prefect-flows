{{
  config(
	materialized = 'incremental',
	unique_key = ['lead_id','aggregate_id','role'],
	)
}}
with cte as (
SELECT
	ca.lead_id,
	pup.aggregate_id,
	concat(pup.first_name,' ',pup.last_name) as name, 
	pup.email,
	pup.phones,
	{% if target.type == 'snowflake' %}
	pup.phones[0].phoneNumber::VARCHAR as phoneNumber,
	pup.phones[0].phoneType::VARCHAR as phoneType,
	{% else %}
	pup.phones->0->>'phoneNumber' as phoneNumber,
	pup.phones->0->>'phoneType' as phoneType,
	{% endif %}
	ca.role,
	pup.updated as partner_updated_at,
	ca.created as current_assigment_created_at 
FROM {{ ref('partner_user_profiles') }}  AS pup 
JOIN {{ ref('current_assignments') }} ca ON profile_aggregate_id=aggregate_id 
{% if is_incremental() %}
WHERE pup.updated >= coalesce((select max(partner_updated_at) from {{ this }}), '1900-01-01')
OR ca.created>= coalesce((select max(current_assigment_created_at) from {{ this }}), '1900-01-01')
{% endif %}),
dedup as(
select distinct
	lead_id,
	aggregate_id,
	name,
	email,
	cast(phones as text) as phones,
	phoneNumber,
	phoneType,
	role,
	partner_updated_at,
	current_assigment_created_at
from
	cte)
select
	lead_id,
	aggregate_id,
	name,
	email,
	{{ parse_json('phones') }} as phones,
	phoneNumber,
	phoneType,
	role,
	partner_updated_at,
	current_assigment_created_at
from dedup	