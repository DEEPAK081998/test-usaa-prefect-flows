WITH valid_emails_cte AS (
    SELECT
        producer_prefix AS valid_producer_prefix
    FROM
        {{ ref('stg_pmc_hierarchy') }}
    ORDER BY
        producer_prefix
),
producer_prefix_cte AS (
    SELECT
        bank_name,
        producer_prefix
    FROM
        {{ ref('pennymac_invites_enrolls_hierarchy') }}
    UNION
    ALL
    SELECT
        bank_name,
        enroll_producer_prefix AS producer_prefix
    FROM
        {{ ref('pennymac_invites_enrolls_hierarchy') }}
),
producer_count_cte AS (
    SELECT
        bank_name,
        producer_prefix,
        count(*) as count
    FROM
        producer_prefix_cte
    WHERE
        producer_prefix {{ not_null_operator() }}
    GROUP BY
        bank_name,
        producer_prefix
    ORDER BY
        count DESC
)
SELECT
    *,
    CASE
        WHEN producer_prefix IN (
            SELECT
                *
            FROM
                valid_emails_cte
        ) THEN TRUE
        ELSE FALSE
    END AS valid_prefix
FROM
    producer_count_cte
ORDER BY
    producer_prefix