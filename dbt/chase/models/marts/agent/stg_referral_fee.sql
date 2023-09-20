{{ config(
    materialized = 'table',
    enabled=true
) }}

WITH lead_fee AS(

    SELECT
        DISTINCT rca.assignee_aggregate_id,
        rca.referral_fee_category_id,
        rfc.description
    FROM
        {{ ref('referral_fee_category_assignments') }} rca
        LEFT JOIN {{ ref('referral_fee_categories') }} rfc
        ON rca.referral_fee_category_id = rfc.referral_fee_category_id
),
-- pull referral percentages from most recent last_effective_date
referral_fee AS(
    SELECT
        DISTINCT
        ON ("referral_fee_category_id") *
    FROM
        {{ ref('referral_fee_categories_rates') }}
    ORDER BY
        "referral_fee_category_id",
        "effective_date" DESC
)
SELECT
    DISTINCT lf.assignee_aggregate_id,
    lf.referral_fee_category_id,
    lf.description,
    rfr.percentage AS percentage_fee
FROM
    lead_fee lf
    LEFT JOIN referral_fee rfr
    ON lf.referral_fee_category_id = rfr.referral_fee_category_id
