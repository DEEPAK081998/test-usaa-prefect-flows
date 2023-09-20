{{
  config(
    materialized = 'table',
  	tags=['pmc_sftp']
    )
}}

WITH

final_cte AS (
    SELECT 
       pp.aggregate_id,
       pp.created,
       pp.updated,
       pp.role,
       pp.first_name,
       pp.last_name,
       pp.email,
       pp.phone,
       pp.license_nmlsid,
       pp.brokerage_code,
       pp.brokerage_name
    FROM {{ ref('pennymac_profiles') }} pp
    WHERE {{ filter_previous_day('created', 'US/Central') }}
    OR {{ filter_previous_day('updated', 'US/Central') }}

)
SELECT * FROM final_cte