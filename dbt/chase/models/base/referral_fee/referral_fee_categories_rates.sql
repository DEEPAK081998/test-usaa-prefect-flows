{{ config(
    materialized = 'table',
) }}

SELECT
    id,
    cast(referral_fee_category_id AS UUID) as referral_fee_category_id,
    effective_date,
    cast(percentage AS INT) percentage,
    created,
    updated
FROM
    {{ source(
        'public',
        'raw_referral_fee_categories_rates'
    ) }}
