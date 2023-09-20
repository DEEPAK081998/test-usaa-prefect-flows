{{
  config(
    materialized = 'table',
    enabled=false
    )
}}
SELECT
  lsu.lead_id
  ,date(t1.created - interval '7 Hours') as enrollDate
  ,date(lsu.created - interval '7 Hours') as updateDate
  ,lsu.category
  ,lsu.status as activity
  ,lsu.email as concierge
  ,lsu.profile->> 'firstName' as conciergeFirstName
  ,lsu.profile->>'lastName' as conciergeLastName
  ,data::json->>'additionalComments'
  ,lb.bank_name
FROM {{ ref('lead_status_updates') }}  lsu JOIN {{ ref('leads') }}  t1 ON lsu.lead_id = t1.id
JOIN {{ ref('stg_lead_banks') }} lb ON t1.bank_id = lb.bank_id
WHERE lsu.category = 'Activities' AND lsu.role = 'ADMIN'