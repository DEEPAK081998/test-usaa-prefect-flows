WITH invites AS(
    SELECT
        id AS msg_id,
        lolastname AS inviting_lolastname,
        lofirstname AS inviting_lofirstname,
        invite_date,
        loemail AS inviting_loemail,
        firstname,
        lastname,
        email,
        purchase_specialist
    FROM
        {{ ref('stg_sofi_invites') }}
),
enrollments AS(
    SELECT
        date AS enrollmentdate,
        id AS lead_id,
        transaction_type,
        hb_status,
        REPLACE(CAST(rc_state AS TEXT),'"','') AS rc_state,
        city,
        state,
        closedate,
        inactivedate,
        lo_email AS enr_lo_email,
        lo_name AS enr_lo_name,
        major_status,
        client_first_name,
        client_last_name,
        client_email,
        agent_name AS agent_name,
        agent_email,
        lastpendingdate,
        last_agent_update,
        hb_status_time
    FROM {{ ref('leads_data_v3') }}
	WHERE bank_name = 'SoFi'
),
combine AS(
	SELECT * FROM invites
	FULL OUTER JOIN enrollments
	ON lower(enrollments.client_email) = lower(invites.email)
),
final AS(
    SELECT
        coalesce(client_email, email) AS client_email_total,
        coalesce(inviting_loemail, enr_lo_email) as loemail,
        *
    FROM
        combine
)
SELECT * FROM final