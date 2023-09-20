{{
  config(
    materialized = 'table',
  	tags=['pmc_sftp']
    )
}}

WITH

final_cte AS (
    SELECT 
      'HS Followup' AS Program,
      ld.bank_name AS Client,
      ld.created,
      ld.client_first_name,
      ld.client_last_name,
      ld.client_email,
      replace(ld.purchase_location,',','') AS "Purchase Location",
      ld.city AS normalizedcity,
      ld.state AS normalizedstate,
      ld.zip AS normalizedzip,
      ld.agentfirstname,
      ld.agentlastname,
      ld.agent_phone,
      ld.agent_email,
      CASE 
        WHEN ld.mlosubmission = 'true' THEN 'MLO Submission' 
        ELSE 'Customer Enrolled'
      END AS enrollmenttype,
      ld.lo_first_name,
      ld.lo_last_name,
      REPLACE(REPLACE(REPLACE(ld.lo_phone,'(',''),')',''),'-','') AS lo_phone,
      ld.lo_email,
      ld.contactmethod,
      ld.client_contact,
      ld.transaction_type,
      SUM(agent_assigned) AS agent_assigned
    FROM {{ ref('leads_data_v3') }} ld
    WHERE ld.bank_name = 'PennyMac'
    AND {{ filter_previous_day('agent_assign_time', 'US/Central') }}
        GROUP BY
      ld.bank_name,
      ld.created,
      ld.client_first_name,
      ld.client_last_name,
      ld.client_email,
      replace(ld.purchase_location,',',''),
      ld.city ,
      ld.state ,
      ld.zip ,
      ld.agentfirstname,
      ld.agentlastname,
      ld.agent_phone,
      ld.agent_email,
      CASE 
        WHEN ld.mlosubmission = 'true' THEN 'MLO Submission' 
        ELSE 'Customer Enrolled'
      END,
      ld.lo_first_name,
      ld.lo_last_name,
      REPLACE(REPLACE(REPLACE(ld.lo_phone,'(',''),')',''),'-',''),
      ld.lo_email,
      ld.contactmethod,
      ld.client_contact,
      ld.transaction_type
)
SELECT * FROM final_cte