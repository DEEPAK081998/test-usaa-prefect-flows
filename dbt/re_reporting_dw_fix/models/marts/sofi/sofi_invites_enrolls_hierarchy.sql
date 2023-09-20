WITH base_cte AS(
    SELECT
        'SoFi' AS bank_name,
        {% if target.type == 'snowflake' %}
        ROW_NUMBER () OVER (ORDER BY NULL) AS row_id,
        CASE
            WHEN lead_id {{ not_null_operator() }} THEN ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY NULL)
            ELSE 1
        END AS row_num,
        {% else %}
        ROW_NUMBER () OVER () AS row_id,
        CASE
            WHEN lead_id {{ not_null_operator() }} THEN ROW_NUMBER() OVER (PARTITION BY lead_id)
            ELSE 1
        END AS row_num,
        {% endif %}
        lead_id,
        msg_id,
        invite_date,
        {{ extract_email_prefix('loemail')}} AS producer_prefix,
        {% if target.type == 'snowflake' %}
        CASE
            WHEN firstname {{ not_null_operator() }} THEN CONCAT(TRIM(firstname, ' '), ' ', TRIM(lastname, ' '))
            ELSE CONCAT(TRIM(client_first_name, ' '), ' ', TRIM(client_last_name, ' '))
            END AS client_name,
        {% else %}
        CASE
            WHEN firstname {{ not_null_operator() }} THEN CONCAT(BTRIM(firstname, ' '), ' ', BTRIM(lastname, ' '))
            ELSE CONCAT(BTRIM(client_first_name, ' '), ' ', BTRIM(client_last_name, ' '))
            END AS client_name,
        {% endif %}
        enrollmentdate AS enroll_date,
        enr_lo_email AS enroll_producer_email,
        {{ extract_email_prefix('enr_lo_email')}} AS enroll_producer_prefix,
        transaction_type,
        hb_status,
        rc_state,
        city,
        state,
        closedate,
        inactivedate,
        major_status,
        agent_name,
        agent_email,
        lastpendingdate,
        last_agent_update,
        purchase_specialist,
        hb_status_time
    FROM
        {{ ref('sofi_lo_report') }}
),
hierarchy_cte AS (
    SELECT
        executive_name,
        manager_name,
        producer_name,
        executive_email,
        manager_email,
        producer_email,
        executive_prefix,
        manager_prefix,
        producer_prefix
    FROM
        {{ ref('stg_sofi_hierarchy') }}
),
invite_hierarchy_join_cte AS (
    SELECT
        *
    FROM
        base_cte
    LEFT JOIN hierarchy_cte USING(producer_prefix)
    WHERE producer_prefix {{ not_null_operator() }} OR enroll_producer_prefix {{ not_null_operator() }}
),
enroll_hierarchy_cte AS (
    SELECT
        producer_prefix AS enroll_producer_prefix,
        executive_name AS enroll_executive_name,
        manager_name AS enroll_manager_name,
        producer_name AS enroll_producer_name
    FROM
        hierarchy_cte
),
invite_enroll_hierarchy_cte AS (
    SELECT
        *
    FROM invite_hierarchy_join_cte
    LEFT JOIN enroll_hierarchy_cte
    USING(enroll_producer_prefix)
),
agg_msg_id_per_lead_id_cte AS(
    SELECT
        lead_id,
        ARRAY_AGG(msg_id) AS msg_ids_leads,
        count(DISTINCT msg_id) AS num_invites_leads
    FROM base_cte
    GROUP BY lead_id
    HAVING lead_id {{ not_null_operator() }}
),
agg_msg_id_per_msg_id_cte AS(
    SELECT
        msg_id,
        ARRAY_AGG(msg_id) AS msg_ids_msgs,
        count(DISTINCT msg_id) AS num_invites_msgs
    FROM (SELECT * FROM base_cte WHERE lead_id {{ is_null_operator() }}) as foo
    GROUP BY msg_id
),
date_cte AS (
    SELECT
        row_id,
        TO_CHAR(enroll_date, 'YYYY-MM') AS enroll_year_month,
        TO_CHAR(invite_date, 'YYYY-MM') AS invite_year_month
    FROM base_cte
)
SELECT
    *,
    COALESCE(msg_ids_leads, msg_ids_msgs) as msg_ids,
    COALESCE(num_invites_leads, num_invites_msgs) as num_invites
    FROM invite_enroll_hierarchy_cte
LEFT JOIN agg_msg_id_per_lead_id_cte USING(lead_id)
LEFT JOIN agg_msg_id_per_msg_id_cte USING(msg_id)
LEFT JOIN date_cte USING(row_id)

