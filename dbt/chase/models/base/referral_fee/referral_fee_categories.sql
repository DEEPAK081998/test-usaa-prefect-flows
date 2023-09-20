{{ config(
    materialized = 'table',
) }}

SELECT
    id,
    cast(partner_id AS UUID) as partner_id,
    cast(referral_fee_category_id as UUID) as referral_fee_category_id,
    name,
    description,
    created,
    updated,
    last_update_id
FROM
    {{ source(
        'public',
        'raw_referral_fee_categories'
    ) }}
