{{
  config(
    materialized = 'table',
    tags=['zillow']
    )
}}

SELECT 'B' as coverageType, 0 as leadID,  0 as agentID, brokerage_code, zip, city, state
FROM {{ ref('brokerage_coverage_zips') }}

UNION

SELECT 'P' as coverageType, 0 as leadID, pup.id as agentID, 'NA' as brokerage_code, pz.zip, pz.city, pz.state
FROM {{ ref('profile_coverage_zips') }} pz JOIN {{ ref('partner_user_profiles') }} pup    on pz.profile_id = pup.id

UNION

SELECT 'E' as coverageType, 0 as leadID, pup.id as agentID,'NA' as brokerage_code, pz.zip, pz.city, pz.state
FROM {{ ref('profile_coverage_zips') }} pz JOIN {{ ref('partner_user_profiles') }} pup    on pz.profile_id = pup.id
WHERE pup.eligibility_status = 'brokerageDirectAssignments'

UNION

{% if target.type == 'snowflake' %}
SELECT 'B-ZHL' as coverageType,t1.id as leadID, 0 as agentID,'NA' as brokerage_code,
      case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:zip::VARCHAR else nl.normalized_purchase_location:zip::VARCHAR end as zip,
      case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:city::VARCHAR else nl.normalized_purchase_location:city::VARCHAR end as city,
      case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:state::VARCHAR else nl.normalized_purchase_location:state::VARCHAR end as state
FROM {{ ref('normalized_lead_locations') }} nl JOIN {{ ref('leads') }} t1 on nl.lead_id = t1.id
WHERE t1.bank_id in ('4377AEDD-2BA1-41EF-9D10-0E522738FD7A')
{% else %}
SELECT 'B-ZHL' as coverageType,t1.id as leadID, 0 as agentID,'NA' as brokerage_code,
      case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'zip' else nl.normalized_purchase_location->>'zip' end as zip,
      case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'city' else nl.normalized_purchase_location->>'city' end as city,
      case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'state' else nl.normalized_purchase_location->>'state' end as state
FROM {{ ref('normalized_lead_locations') }} nl JOIN {{ ref('leads') }} t1 on nl.lead_id = t1.id
WHERE t1.bank_id in ('4377AEDD-2BA1-41EF-9D10-0E522738FD7A')
{% endif %}