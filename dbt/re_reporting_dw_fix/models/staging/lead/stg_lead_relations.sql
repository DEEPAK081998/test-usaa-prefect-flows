{{
  config(
    materialized = 'view',
    )
}}
select 
	distinct 
	pup.id,
	pup.aggregate_id,
	pur.{{  special_column_name_formatter('role') }},
	pup.last_name,
	pup.first_name,
	pup.first_name  || ' ' ||pup.last_name  as full_name,
	pup.partner_id,
	spb.bank_name,
	ca.lead_id
from {{ ref('partner_user_profiles') }} pup 
join {{ ref('partner_user_roles') }} pur 
on pup.id = pur.user_profile_id
left join {{ ref('stg_pup_banks') }} spb 
on pup.partner_id = spb.partner_id 
left join {{ ref('current_assignments') }} ca 
on pup.aggregate_id = ca.profile_aggregate_id 