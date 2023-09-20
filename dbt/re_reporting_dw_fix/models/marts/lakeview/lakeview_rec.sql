{{
  config(
    materialized = 'view',
  	tags=['lakeview','rec']
    )
}}

WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec') }}
    WHERE bank_id in ('05eff75c-b274-49b5-94a0-9f47621d5d16')
)
SELECT * FROM final_cte