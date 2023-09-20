{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}}
SELECT  
    city,
    created,
    id,
    profile_id,
    cast(state as text) as state
FROM 
    {{ source('public', 'raw_profile_cities') }}