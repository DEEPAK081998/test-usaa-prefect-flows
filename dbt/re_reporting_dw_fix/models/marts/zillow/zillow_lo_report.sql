WITH enrollments AS(
    SELECT
        id as lead_id,
        transaction_type,
        date AS enrollmentdate,
        id,
        hb_status,
        REPLACE(CAST(rc_state AS TEXT),'"','') AS rc_state,
        city,
        state,
        closedate,
        inactivedate,
        lo_email AS loemail,
        lo_name,
        client_email,
        major_status,
        client_first_name,
        client_last_name,
        client_email AS client_email_total,
        agent_name AS agent_name,
        lastpendingdate,
        agent_email,
        last_agent_update
    FROM {{ ref('leads_data_v3') }}
	WHERE bank_name = 'Zillow'
)
SELECT * FROM enrollments