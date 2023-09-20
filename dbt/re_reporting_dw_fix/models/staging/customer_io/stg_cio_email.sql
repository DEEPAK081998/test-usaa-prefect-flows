WITH email AS(
select 
	cio_id,
    customer_id as aggregate_id,
	{{  special_column_name_formatter('role') }},
    recipient,
	campaign_id,
	{{  special_column_name_formatter('name') }} as campaign_type,
	campaign_type as {{  special_column_name_formatter('name') }},
	medium,
	bank_name,
    customer_name,
	COUNT({{  special_column_name_formatter('sent') }}) as total_email_sent,
	COUNT(delivered) as total_email_recieved,
	COUNT(opened) as total_email_opened,
	COUNT(clicked) as total_email_clicks,
    CAST(NULL AS bigint) AS count_sms_responses,
    CAST(NULL AS timestamp) AS message_first_sent_date
FROM
	{{ ref('stg_cio_messages') }} scm
where medium = 'email'
group by
	cio_id,
    customer_id,
	{{  special_column_name_formatter('role') }},
    recipient,
	campaign_id,
	{{  special_column_name_formatter('name') }},
	campaign_type,
	medium,
	bank_name,
    customer_name,
    count_sms_responses  
),
add_gs_sheet AS(
SELECT
    cio_id,
    cast(aggregate_id as {{ uuid_formatter() }}) as aggregate_id,
    role,
    recipient,
    email.campaign_id,
    name,
    campaign_type,
    medium,
    bank_name,
    customer_name,
    total_email_sent,
    total_email_recieved as total_email_received,
    total_email_opened,
    total_email_clicks,
    count_sms_responses,
    message_first_sent_date,
    csat,
    sales,
    closes,
    status,
    network,
    audience,
    priority,
    use_cause,
    close_rate,
    enrollment,
    dependencies,
    communication,
    copy_complete,
    journey_section,
    efficiency_impact,
    active_enrollments,
    verified_enrollments,
    link_to_overview_docs,
    partner_attachement_rate
FROM    
    email
LEFT JOIN {{ source('public', 'raw_campaigns') }} rc
ON email.campaign_id=cast(rc.campaign_id AS bigint) AND lower(email.role)=lower(rc.audience)
)
SELECT * FROM add_gs_sheet
