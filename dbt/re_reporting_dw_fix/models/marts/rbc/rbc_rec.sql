{{
  config(
    materialized = 'view',
  	tags=['rbc','rec']
    )
}}

WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec') }}
    WHERE bank_id in ('6E15BA77-A19A-4CCC-ACC6-3AA3601FCBF9')
)
SELECT * FROM final_cte