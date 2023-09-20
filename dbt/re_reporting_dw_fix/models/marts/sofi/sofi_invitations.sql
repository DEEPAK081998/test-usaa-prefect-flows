{{
  config(
	materialized = 'view',
	tags=['sofi']
	)
}}
SELECT
	lead_data_unpack.lead_id,
	lead_data_unpack.data AS leads_data,
	lead_data_unpack.created AS lead_created,
	lead_data_unpack.updated AS lead_updated,
	partner_unpack.first_name,
	partner_unpack.last_name,
	partner_unpack.email,
	partner_unpack.phone,
	partner_unpack.created AS partner_details_created,
	partner_unpack.updated AS partner_details_updated,
	partner_unpack.brokerage_code,
	partner_unpack.local_timezone,
	partner_unpack.eligibility_status
FROM {{ ref('stg_sofi_lead_data_unpack') }} lead_data_unpack
INNER JOIN {{ ref('stg_sofi_partner_unpack') }} partner_unpack
ON lead_data_unpack.leads_leadid = partner_unpack.lead_id