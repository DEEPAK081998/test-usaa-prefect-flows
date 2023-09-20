{{
  config(
    materialized = 'view',
  	tags=['zillow','rec']
    )
}}
WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec_w_statuses') }}
    WHERE bank_id in ('4377AEDD-2BA1-41EF-9D10-0E522738FD7A')
)
SELECT * FROM final_cte