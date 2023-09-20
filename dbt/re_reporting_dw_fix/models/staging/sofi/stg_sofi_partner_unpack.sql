{{
  config(
    materialized = 'view',
    )
}}
WITH
partner_data AS(
{% if target.type == 'snowflake' %}
    SELECT *,
        data:sofi AS sofi_data
    FROM {{ ref('partner_user_profiles') }}
    WHERE data:sofi IS NOT NULL
{% else %}
    SELECT *,
        data->'sofi' AS sofi_data
    FROM {{ ref('partner_user_profiles') }}
    WHERE data->'sofi' IS NOT NULL
{% endif %}
)
, partner_unpack AS (
{% if target.type == 'snowflake' %}
    SELECT *,
        sofi_data:partyId::VARCHAR AS party_id,
        sofi_data:leadId::VARCHAR AS lead_id
    FROM partner_data
{% else %}
    SELECT *,
        CAST(sofi_data->'partyId' AS VARCHAR) AS party_id,
        CAST(sofi_data->'leadId' AS VARCHAR) AS lead_id
    FROM partner_data
{% endif %}
)
SELECT * FROM partner_unpack