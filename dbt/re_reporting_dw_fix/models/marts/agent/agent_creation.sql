SELECT 
    pup.id,
    pup.aggregate_id,
    pup.brokerage_code,
    pup.created,
    pup.eligibility_status
FROM {{ ref('partner_user_profiles') }} pup
    JOIN {{ ref('partner_user_roles') }} pur on pur.id = pup.id
WHERE pur.role = 'AGENT'
