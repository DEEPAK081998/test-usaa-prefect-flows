WITH invite_count_cte AS (
    SELECT
        bank_name,
        executive_name,
        manager_name,
        producer_name,
        count(DISTINCT msg_id) AS invites_count,
        array_agg(DISTINCT msg_id) AS invites_msg_id
    FROM
        (SELECT * FROM {{ ref('pennymac_invites_enrolls_hierarchy') }}
            WHERE msg_id {{ not_null_operator() }} AND (invite_date < current_date OR invite_date {{ is_null_operator() }})
        ) AS bar
    GROUP BY
        bank_name,
        executive_name,
        manager_name,
        producer_name
),
enroll_count_cte AS (
    SELECT
        bank_name,
        enroll_executive_name AS executive_name,
        enroll_manager_name AS manager_name,
        enroll_producer_name AS producer_name,
        count(DISTINCT lead_id) AS enrolls_count,
        array_agg(DISTINCT lead_id) AS enrolls_lead_id
    FROM
        (
            SELECT
                *
            FROM
                {{ ref('pennymac_invites_enrolls_hierarchy') }}
            WHERE
                lead_id {{ not_null_operator() }} AND (enroll_date < current_date OR enroll_date {{ is_null_operator() }})
        ) AS foo
    GROUP BY
        bank_name,
        enroll_executive_name,
        enroll_manager_name,
        enroll_producer_name
)
SELECT
    bank_name,
    executive_name,
    manager_name,
    producer_name,
    invites_count,
    enrolls_count,
    invites_msg_id,
    enrolls_lead_id
FROM
    invite_count_cte
    FULL JOIN enroll_count_cte USING(
        bank_name,
        executive_name,
        manager_name,
        producer_name
    )
ORDER BY
    bank_name,
    executive_name,
    manager_name,
    producer_name