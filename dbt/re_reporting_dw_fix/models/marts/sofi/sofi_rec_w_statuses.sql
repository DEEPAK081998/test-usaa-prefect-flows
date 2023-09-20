{{
  config(
    materialized = 'view',
  	tags=['sofi','rec']
    )
}}
WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec_w_statuses') }}
    WHERE bank_id in ('77DACCC1-1178-4FD2-B95E-4F291476CBD9')
)
SELECT * FROM final_cte