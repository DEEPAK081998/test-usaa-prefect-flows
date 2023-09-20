{{
  config(
    materialized = 'view',
  	tags=['freedom','rec']
    )
}}

WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec') }}
    WHERE bank_id in ('3405dc7c-e972-4bc4-a3da-cb07e822b7c6','482c83b7-12f3-4098-a846-b3091a33f966')
)
SELECT * FROM final_cte