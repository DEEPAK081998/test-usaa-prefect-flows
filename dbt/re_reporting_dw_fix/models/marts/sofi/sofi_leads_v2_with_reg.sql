{{
  config(
	materialized = 'view',
	tags=['sofi']
	)
}}
WITH total_sofi_registrations AS(
	SELECT *
	FROM {{ ref('stg_partners_total_registrations') }}
	WHERE partner_id = '77daccc1-1178-4fd2-b95e-4f291476cbd9'
)
SELECT *,'SoFi' AS bank_partner FROM total_sofi_registrations