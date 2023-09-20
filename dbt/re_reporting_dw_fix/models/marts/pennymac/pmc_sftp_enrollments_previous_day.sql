{{
  config(
    materialized = 'table',
  	tags=['pmc_sftp']
    )
}}

WITH

final_cte AS (
    SELECT 
      'PennyMac' AS bank_name,
      pil.id,
      pil.transaction_type,
      pil.date,
      pil.client_aggregate_id,
      pil.agent_aggregate_id,
      pil.lo_aggregate_id,
      pil.city,
      pil.state,
      pil.zip,
      CASE 
        WHEN pil.activity_type IS NULL THEN 'Self-Enrolled'
        ELSE pil.activity_type 
      END  AS "Enrollment Type",
      pil.contactmethod,
      CASE 
        WHEN pil.agent_assigned = 1 THEN 'True'
        ELSE 'False'
      END AS "Agent Assigned"
     FROM {{ ref('pennymac_invited_leads') }} pil
    WHERE {{ filter_previous_day('date', 'US/Central') }}
    OR {{ filter_previous_day('agent_assign_time', 'US/Central') }}
    OR {{ filter_previous_day('mlo_assign_time', 'US/Central') }}

)
SELECT * FROM final_cte