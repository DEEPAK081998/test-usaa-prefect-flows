{{
  config(
	materialized='view'
	)
}}
SELECT
	pup.lead_id as leadID,
	pup.*
FROM {{ ref('stg_sofi_partner_unpack') }} pup
JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
WHERE pur.role = 'CUSTOMER'