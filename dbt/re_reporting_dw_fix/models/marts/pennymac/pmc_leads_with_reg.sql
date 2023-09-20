{{
  config(
	materialized = 'view',
	tags=['pmc']
	)
}}
WITH total_pm_registrations AS(
	SELECT *
	FROM {{ ref('stg_partners_total_registrations') }}
	WHERE partner_id = 'E2A46D0A-6544-4116-8631-F08D749045AC'
)
SELECT *,'PennyMac' AS bank_partner FROM total_pm_registrations