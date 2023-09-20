{{
  config(
    materialized = 'view',
    )
}}
SELECT 
    *,
    CONCAT(firstname,' ',lastname) as customer_name,
    id AS invitation_id,
    CONCAT(lofirstname, ' ', lolastname) AS lo_name
FROM {{ ref('stg_freedom_invites') }} 