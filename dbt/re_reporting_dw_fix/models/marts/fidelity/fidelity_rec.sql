{{
  config(
    materialized = 'view',
  	tags=['fidelity','rec']
    )
}}
WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec') }}
    WHERE bank_id in ('3EEAEF5F-6F12-4B42-BB7B-3B0EB74E4B1F')
)
SELECT * FROM final_cte