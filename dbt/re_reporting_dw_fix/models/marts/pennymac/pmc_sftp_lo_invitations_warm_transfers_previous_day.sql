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
    pl.invite_id,
    pl.activity_date AS date,
    split_part(pl.invitee_name,' ',1) AS client_first_name,
    CASE
      WHEN split_part(pl.invitee_name,' ',2) = '' THEN split_part(pl.invitee_name,'  ',2)
      ELSE split_part(pl.invitee_name,' ',2) 
    END AS client_last_name,
    pl.client_email,
    pl.client_phone,
    pl.client_aggregate_id,
    pl.lo_aggregate_id
  FROM {{ ref('pennymac_lo_invitations_warm_transfers') }} pl
  WHERE {{ filter_previous_day('activity_date', 'US/Central') }}
)
SELECT * FROM final_cte