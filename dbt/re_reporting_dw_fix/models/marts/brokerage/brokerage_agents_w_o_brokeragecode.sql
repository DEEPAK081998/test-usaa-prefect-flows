SELECT
    pup.id,
    pup.first_name as AgentFirstName,
    pup.last_name as AgentLastName,
    pup.eligibility_status,
    concat(pup.first_name,' ',pup.last_name) as AgentName,
    pup.email as AgentEmail,
    pup.phone as AgentPhone,
    pup.brokerage_code as BrokerageCode
FROM {{ ref('partner_user_profiles') }} pup
JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
WHERE lower(pur.role) = 'agent'