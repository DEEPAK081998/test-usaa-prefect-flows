{{
  config(
    materialized = 'view',
  	tags=['alliant','rec']
    )
}}

WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec_w_statuses') }}
    WHERE bank_id in ('1046a4d7-a3af-4533-8cf0-a02210b94ba1')
)
SELECT * FROM final_cte