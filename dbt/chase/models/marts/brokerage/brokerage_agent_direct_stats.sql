{{
  config(
    materialized = 'view',
  	tags=['brokerage'],
    enabled=false
    )
}}
SELECT
    brokerage_code,
    full_name,
    rc_name,
    rc_email,
    rc_phone,
    coverage,
    brokeragequalification,
    agentdirectDate,
    agentCount,
    eligibleAgentCount,
    count(id) as acceptedReferrals,
    sum(unassignedEnrollments) as unassignedEnrollments,
    sum(unacceptedEnrollments) as unacceptedEnrollments
FROM {{ ref('stg_brokerage_leads') }} a
GROUP BY
    brokerage_code,
    full_name,
    rc_name,
    rc_email,
    rc_phone,
    coverage,
    brokeragequalification,
    agentdirectDate,
    agentCount,
    eligibleAgentCount