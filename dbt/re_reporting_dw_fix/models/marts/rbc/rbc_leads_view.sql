{{
  config(
    materialized = 'view',
    )
}}
SELECT * 
FROM {{ ref('leads_data_v3') }}
WHERE bank_name ='RBC'