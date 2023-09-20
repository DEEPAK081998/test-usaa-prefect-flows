{% set schema_value = 'OUTBOUND' if target.system == 'snowflake' else 'public' %}

{{ config(
    materialized = 'table',
    schema = schema_value
) }}


WITH CTE AS (
    SELECT 
        aggregate_id,
        ROW_NUMBER() OVER (ORDER BY email) as rn,
        COUNT(*) OVER () as total
    FROM {{ ref('partner_user_profiles') }} pup 
    join {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
    where pur.role = 'AGENT'
)

SELECT
    aggregate_id,
    CASE
    
        WHEN rn <= total * 0.05 THEN 0
        WHEN rn <= total * 0.15 THEN 1
        WHEN rn <= total * 0.25 THEN 2
        WHEN rn <= total * 0.75 THEN 3
        WHEN rn <= total * 0.85 THEN 4
        WHEN rn <= total * 0.95 THEN 5
        ELSE 6
    END as score
FROM CTE