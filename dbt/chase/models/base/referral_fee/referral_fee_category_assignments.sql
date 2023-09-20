{{ config(
    materialized = 'table',
) }}

SELECT
    id,
    assignee_aggregate_id::uuid as assignee_aggregate_id,
    cast(referral_fee_category_id as UUID) as referral_fee_category_id,
    created,
    updated
FROM
    {{ source(
        'public',
        'raw_referral_fee_category_assignments'
    ) }}
