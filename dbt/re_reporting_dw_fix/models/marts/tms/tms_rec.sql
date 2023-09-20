{{
  config(
    materialized = 'view',
  	tags=['tms','rec']
    )
}}

WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec') }}
    WHERE bank_id in ('1085d3ef-3f2b-499a-92c5-71a12df5a7ba')
)
SELECT * FROM final_cte