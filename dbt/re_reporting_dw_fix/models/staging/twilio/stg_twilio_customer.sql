WITH customer_data AS(
SELECT 
    ta.*,
    pup.email as pup_email,
    CASE WHEN pup.first_name IS NOT NULL AND pup.last_name IS NOT NULL
        THEN CONCAT(pup.first_name, ' ', pup.last_name) ELSE NULL END AS pup_name,
    l.email AS lead_email,
    CASE WHEN l.first_name IS NOT NULL AND l.last_name IS NOT NULL
        THEN CONCAT(l.first_name, ' ', l.last_name) ELSE NULL END AS l_name,
    pup.partner_id,
    l.bank_id,
    lb.bank_name
{% if target.type == 'snowflake' %}
FROM
    {{ ref('twilio_analysis') }} ta
{% else %}
FROM
    twilio_analysis ta
{% endif %}
LEFT JOIN {{ ref('partner_user_profiles') }} pup
ON ta.pup_id=pup.id
LEFT JOIN {{ ref('leads') }} l 
ON ta.lead_id=l.id
LEFT JOIN {{ ref('stg_pup_banks') }} lb
ON pup.partner_id=lb.partner_id
),
leads_bank_name AS(
SELECT
    cd.*,
    lb.bank_name AS leads_bank_name
FROM
    customer_data cd
LEFT JOIN {{ ref('stg_lead_banks') }} lb
ON cd.bank_id=lb.bank_id
),
all_data AS(
SELECT
    twilio_phone_id as phone,
    pup_id,
    lead_id,
    partner_role AS role,
    twilio_total_received AS total_sms_received,
    twilio_total_sent AS total_sms_sent,
    coalesce(lead_email, pup_email) AS email,
    coalesce(l_name, pup_name) AS name,
    coalesce(leads_bank_name, bank_name) AS partner
FROM leads_bank_name
)
SELECT * FROM all_data