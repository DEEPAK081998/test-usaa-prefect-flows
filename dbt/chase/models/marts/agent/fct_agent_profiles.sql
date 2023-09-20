{{
  config(
    materialized = 'table',
    enabled=true
    )
}}

SELECT
    *
FROM 
    {{ ref('stg_agent_profiles') }} stg_agent_profiles
LEFT JOIN {{ ref('stg_hla_hierarchy') }} hla
ON stg_agent_profiles.aggregate_id =  hla.agentAggregateID
LEFT JOIN {{ ref('stg_referral_fee') }} stg_referral_fee
ON stg_agent_profiles.aggregate_id = stg_referral_fee.assignee_aggregate_id