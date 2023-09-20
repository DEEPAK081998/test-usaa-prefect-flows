WITH lead_status AS (
    SELECT
        lead_id,
        MIN(created) AS agent_connection_date_lsu
    FROM
        {{ ref('lead_status_updates') }}
    WHERE
        category LIKE 'Property%'
        AND status = 'Active Actively Searching'
    GROUP BY
        1
),
lead_status_cio AS (
    SELECT
        lead_id :: bigint AS lead_id,
        MIN(reporting_Date) AS agent_connection_date_cio
    FROM
        {{ ref('base_cio_campaign_sms_response') }}
    WHERE
        campaign_id = 21
        AND response ILIKE '%1 - Actively working with customer%'
    GROUP BY
        1
)
SELECT
    id AS lead_id,
    reporting_enroll_date,
    agent_assigned,
    agent_aggregate_id,
    bank_name,
    hb_status,
    major_status,
    closedate,
    inactivedate,
    agent_connection_date_lsu,
    agent_connection_date_cio
FROM
    {{ ref('leads_data_v3') }} ldv
    LEFT JOIN lead_status ls
    ON ldv.id = ls.lead_id
    LEFT JOIN lead_status_cio lsc
    ON ldv.id = lsc.lead_id
