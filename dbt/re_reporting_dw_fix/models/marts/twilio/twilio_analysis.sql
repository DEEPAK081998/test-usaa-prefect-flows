SELECT
	twilio_phone_id,
	pup_id,
	lead_id,
	{{  special_column_name_formatter('role') }} AS partner_role,
	transaction_type AS lead_transaction,
	total_received AS twilio_total_received,
	total_sent AS twilio_total_sent,
	to_avg_rating AS twilio_to_avg_rating,
	from_avg_rating AS twilio_from_avg_rating,
	'sms' AS medium
FROM {{  ref('stg_link_twilio_pup_lead_client') }} AS link
LEFT JOIN {{ ref('partner_user_roles') }} AS partner
	ON link.pup_id =  partner.user_profile_id
LEFT JOIN {{ ref('leads_data_v3') }} AS leads
	ON link.lead_id =  leads.id
LEFT JOIN {{ ref('stg_twilio_aggregated') }} AS twilio
	using(twilio_phone_id)
