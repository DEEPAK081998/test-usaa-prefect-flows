{{ config(
    materialized = 'table',
) }}

SELECT
    lsu.profile_aggregate_id AS agent_aggregate_id,
    lsu.lead_id,
    lsu.id AS updateID,
    lsu.created,
    lsu.status,
    lsu.category,
    {% if target.type == 'snowflake' %}
    lsu.data:twilioEvent::VARCHAR AS twilioEvent,
    {% else %}
    lsu.data->>'twilioEvent' AS twilioEvent,
    {% endif %}
    t1.enrolleddate,
    stg.bank_name as program_name,
    stg.bank_id as bank_id,
    t1.normalizedclosedate,
    t1.majorstatus,
    t1.inviteddate,
    {% if target.type == 'snowflake' %}
    CASE
        WHEN t1.transaction_type = 'SELL' THEN nl.normalized_sell_location:zip::VARCHAR
        ELSE nl.normalized_purchase_location:zip::VARCHAR
    END AS referralZip,
    CASE
        WHEN DATA:twilioEvent::VARCHAR = 'reservation.accepted' THEN 'Accepted'
        WHEN DATA:twilioEvent::VARCHAR = 'reservation.timeout' THEN 'Timeout'
        WHEN DATA:twilioEvent::VARCHAR = 'reservation.rejected' THEN 'Reject'
        WHEN DATA:twilioEvent::VARCHAR = 'reservation.created' THEN 'Offered'
    {% else %}
    CASE
        WHEN t1.transaction_type = 'SELL' THEN nl.normalized_sell_location->>'zip'
        ELSE nl.normalized_purchase_location->>'zip'
    END AS referralZip,
    CASE
        WHEN DATA->>'twilioEvent' = 'reservation.accepted' THEN 'Accepted'
        WHEN DATA->>'twilioEvent' = 'reservation.timeout' THEN 'Timeout'
        WHEN DATA->>'twilioEvent' = 'reservation.rejected' THEN 'Reject'
        WHEN DATA->>'twilioEvent' = 'reservation.created' THEN 'Offered'
    {% endif %}
        WHEN status LIKE 'Outreach Click to Call' THEN 'CTC'
        WHEN status LIKE 'Closed%' THEN 'Close'
        WHEN lsu.category LIKE 'Property%'
        AND LOWER(status) NOT LIKE ('%assigned%')
        AND (status LIKE ('Inactive%')
        OR status LIKE ('Active%')
        OR status LIKE ('Closed Closed')
        OR status LIKE ('Pending%')
        OR status LIKE ('On Hold')) THEN 'Update'
        ELSE 'Other'
    END AS agentActivity
FROM
    {{ ref('lead_status_updates') }} lsu
JOIN {{ ref("chase_enrollments_test") }} t1
    ON lsu.lead_id = t1.id
left join {{ ref('leads') }} t2 
   ON t1.id = t2.id
left join {{ ref('stg_lead_banks') }} stg 
   on t2.bank_id = stg.bank_id
LEFT OUTER JOIN {{ ref("normalized_lead_locations") }} nl
    ON lsu.lead_id = nl.lead_id
WHERE
    t1.consumer_confirmed = TRUE
