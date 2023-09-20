WITH sms AS(
select
    stc.phone,
    pup_id,
    lead_id,
    role,
    total_sms_received,
    total_sms_sent,
    pup.aggregate_id as pup_agg_id,
    CAST(l.aggregate_id as {{ uuid_formatter() }}) AS lead_agg_id,
    CAST(NULL as {{ uuid_formatter() }}) as agg_id,
    stc.email,
    stc.name,
    stc.partner,
    'sms' AS medium,
    CAST(NULL AS bigint) as transactional_message_id,
    CAST(NULL AS bigint) as total_email_sent,
    CAST(NULL AS bigint) as total_email_received,
    CAST(NULL AS bigint) as total_email_opened,
    CAST(NULL AS bigint) as total_email_clicks
FROM {{ ref('stg_twilio_customer') }} stc
LEFT JOIN {{ ref('partner_user_profiles') }} pup
ON stc.pup_id=pup.id
LEFT JOIN {{ ref('leads') }} l
ON stc.lead_id=l.id
),
email AS(
select 
    profile_phone AS phone,
	pup_id,
    lead_id,
	{{  special_column_name_formatter('role') }},
    CAST(NULL AS bigint) AS total_sms_received,
    CAST(NULL AS bigint) AS total_sms_sent,
    CAST(NULL as {{ uuid_formatter() }}) AS pup_agg_id,
    CAST(NULL as {{ uuid_formatter() }}) AS lead_agg_id,
    CAST(customer_id as {{ uuid_formatter() }}) AS agg_id,
    customer_email AS email,
    customer_name as name,
    bank_name AS partner,
	medium,
    transactional_message_id,
	COUNT({{  special_column_name_formatter('sent') }}) as total_email_sent,
	COUNT(delivered) as total_email_received,
	COUNT(opened) as total_email_opened,
	COUNT(clicked) as total_email_clicks
from 
	{{ ref('stg_cio_messages') }} scm
where medium = 'email'
group by
    profile_phone,
    pup_id,
    lead_id,
    {{  special_column_name_formatter('role') }},
    pup_agg_id,
    lead_agg_id,
    agg_id,
    total_sms_received,
    total_sms_sent,
    email,
    name,
    partner,
    medium,
	campaign_type,
	medium,
    transactional_message_id,
	bank_name,
    customer_name
),
union_table AS(
SELECT * FROM sms
UNION ALL
SELECT * FROM email
)
SELECT 
    phone,
    pup_id AS profile_id,
    lead_id,
    role,
    total_sms_received,
    total_sms_sent,
    coalesce(agg_id, lead_agg_id, pup_agg_id) AS aggregate_id,
    email,
    name,
    partner,
    medium,
    transactional_message_id,
    total_email_sent,
    total_email_received,
    total_email_opened,
    total_email_clicks
FROM union_table