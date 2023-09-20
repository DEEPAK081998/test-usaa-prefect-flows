{{
  config(
    materialized = 'view',
  	tags=['pmc','rec']
    )
}}
WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec_w_statuses') }}
    WHERE bank_id in ('E2A46D0A-6544-4116-8631-F08D749045AC') and agent_submitted = 'true'
)
SELECT * FROM final_cte