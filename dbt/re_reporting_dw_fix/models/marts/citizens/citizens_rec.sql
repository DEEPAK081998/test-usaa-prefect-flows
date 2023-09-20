{{
  config(
    materialized = 'view',
  	tags=['citizens','rec']
    )
}}

WITH

final_cte AS (
    SELECT * FROM {{ ref('stg_rec') }}
    WHERE bank_id in ('06D7EEA6-A312-4233-B53B-DE52EA1C240E')
)
SELECT * FROM final_cte