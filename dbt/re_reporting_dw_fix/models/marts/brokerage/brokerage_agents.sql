{{
  config(
    materialized = 'incremental',
    unique_key = ['id','state','county'],
  	tags=['brokerage']
    )
}}
WITH

current_assignments_cte AS (
    SELECT ca.profile_aggregate_id, count(ca.lead_id) as leadCount
    FROM {{ ref('current_assignments') }} ca
    GROUP BY ca.profile_aggregate_id
)
, current_lead_statuses_cte AS (
    SELECT ca.profile_aggregate_id, count (cls.lead_id) as agentClosedCount
    FROM {{ ref('current_lead_statuses') }} cls
    JOIN {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
    WHERE category like 'Property%' and status like 'Closed%'
    GROUP BY ca.profile_aggregate_id
)

SELECT DISTINCT
    pup.id,
    pup.first_name as AgentFirstName,
    pup.last_name as AgentLastName,
    pup.eligibility_status,
    concat(pup.first_name,' ',pup.last_name) as AgentName,
    pup.email as AgentEmail,
    pup.phone as AgentPhone,
    pup.brokerage_code as BrokerageCode,
    b.full_name,
    bcz.state,
    bcz.county,
    lc.leadCount,
    agentClosedCounts.agentClosedCount,
    {{ current_date_time() }} AS updated_at
FROM {{ ref('partner_user_profiles') }} pup
JOIN {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
JOIN {{ ref('brokerage_coverage_zips') }} bcz on bcz.brokerage_code = b.brokerage_code
JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
LEFT OUTER JOIN current_assignments_cte lc on lc.profile_aggregate_id = pup.aggregate_id
LEFT OUTER JOIN current_lead_statuses_cte agentClosedCounts on agentClosedCounts.profile_aggregate_id = pup.aggregate_id
WHERE lower(pur.role) = 'agent'
{% if is_incremental() %}
  and pup.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}