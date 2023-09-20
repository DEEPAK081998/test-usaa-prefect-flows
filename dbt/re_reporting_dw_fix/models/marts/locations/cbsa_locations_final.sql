{{
  config(
    materialized = 'table',
    )
}}

select DISTINCT lpad(zip, 5, '0') as ZIP,
st as ST,
county as COUNTY,
cbmsa as CBMSA,
city as CITY
from {{ ref('cbsa_locations') }}
order by 1 ASC