{{
  config(
    materialized = 'view',
    enabled=false
    )
}}
SELECT
    DISTINCT lpad(zip, 5, '0') as ZIP,
    st as ST,
    county as COUNTY,
    cbmsa as CBMSA,
    city as CITY
FROM {{ source('public', 'cbsa_locations') }}
ORDER BY 1 ASC