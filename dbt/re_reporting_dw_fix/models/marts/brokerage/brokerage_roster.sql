WITH
current_assigments_cte AS (
    SELECT ca.profile_aggregate_id, count(ca.lead_id) as leadCount
    FROM {{ ref('current_assignments') }} ca
    GROUP BY ca.profile_aggregate_id
)
SELECT
    pup.id,
    pup.aggregate_id,
    pup.first_name as AgentFirstName,
    pup.last_name as AgentLastName,
    pup.eligibility_status,
    concat(pup.first_name,' ',pup.last_name) as AgentName,
    pup.email as AgentEmail, pup.phone as AgentPhone,
    b.brokerage_code as BrokerageCode,
    lc.leadCount,
    b.full_name as BrokerageName
FROM {{ ref('brokerages') }} b
JOIN {{ ref('partner_user_profiles') }} pup on pup.brokerage_code = b.brokerage_code
JOIN {{ ref('partner_user_roles') }} pur on pur.id = pup.id
LEFT OUTER JOIN current_assigments_cte lc on lc.profile_aggregate_id = pup.aggregate_id
WHERE pur.role = 'AGENT'