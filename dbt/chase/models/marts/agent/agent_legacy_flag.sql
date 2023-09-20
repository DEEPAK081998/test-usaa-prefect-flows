{{
  config(
    materialized = 'table',
    enabled=false
    )
}}
WITH agent_data AS(
SELECT
	pup.id,
	pup.first_name AS agentfirstname,
	pup.last_name AS agentlastname,
	pup.eligibility_status,
	concat(pup.first_name, ' ', pup.last_name) AS agentfullname,
	pup.email AS agentemail,
	pup.phone AS agentphone,
	pup.brokerage_code as brokeragecode,
	b.full_name AS brokeragename,
	REPLACE(CAST(pup.data::json->'referralAgreementEmail' AS TEXT), '"','') AS referralagreementemail,
	REPLACE(CAST(pup.data::json->'brokerage'->'email' AS TEXT), '"', '') AS brokerageemail,
	REPLACE(CAST(pup.data::json->'brokerage'->'phones'->0->'phoneNumber' AS TEXT), '"', '') AS brokerage_phone,
	CAST(pup.data::json->'inviter'->'email' AS TEXT) AS inviter_email,
	CAST(pup.data::json->'caeLegacyAgent' AS TEXT) AS caeLegacyAgent,
    REPLACE(CAST(pup.data::json->'verificationStatus' AS TEXT),'"','') as verificationstatus
FROM
	partner_user_profiles pup
LEFT JOIN brokerages b on b.brokerage_code = pup.brokerage_code
JOIN partner_user_roles pur on pur.user_profile_id = pup.id
WHERE pur.role = 'AGENT'
),
add_legacy_flag AS(
	SELECT
		*,
		CASE
			WHEN caelegacyagent = 'true' THEN 'true' ELSE 'false' END as cae_legacy_agent
	FROM
		agent_data
)
SELECT * FROM add_legacy_flag
