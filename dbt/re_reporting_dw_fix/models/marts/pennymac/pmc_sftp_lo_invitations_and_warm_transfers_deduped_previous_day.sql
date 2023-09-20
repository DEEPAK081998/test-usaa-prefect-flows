{{
  config(
    materialized = 'table',
  	tags=['pmc_sftp']
    )
}}

WITH

final_cte AS (
    SELECT 
      'Pennymac TOF' AS Program,
      'Pennymac' AS Client,
      plo.invite_id AS id,
      plo.activity_date AS "Invite Date",
      plo.leadid,
      plo.transaction_type,
      plo.firstname AS client_first_name,
      plo.lastname AS client_last_name,
      plo.invite_email AS client_email,
      plo.phonenumber AS client_phone,
      plo.purchase_location AS "Purchase Location",
      plo.city AS normalizedcity,
      plo.state AS normalizedstate,
      plo.zip AS normalizedzip,
      plo.agent_name,
      plo.agent_phone,
      plo.agent_email,
      plo.activity_type AS enrollmenttype,
      plo.lofirstname AS lo_first_name,
      plo.lolastname AS lo_last_name,
      plo.lo_aggregate_id AS lo_aggregate_id_final,
      plo.lophonenumber AS lo_phone,
      plo.loemail AS lo_email     
    FROM {{ ref('pennymac_lo_invitations_and_warm_transfers_deduped') }} plo
    WHERE {{ filter_previous_day('activity_date', 'US/Central') }}
    AND lower(plo.loemail) NOT LIKE '%freedom%'
    AND lower(plo.activity_type) != 'invite'
)
SELECT * FROM final_cte