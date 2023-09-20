SELECT 
    *
FROM 
    {{ ref('agent_profiles') }} ap
    LEFT JOIN {{ ref('referral_fee') }} rf
    ON ap.aggregate_id=rf.assignee_aggregate_id