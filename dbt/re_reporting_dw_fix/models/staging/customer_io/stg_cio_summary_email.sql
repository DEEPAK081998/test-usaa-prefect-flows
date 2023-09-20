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
	{{  special_column_name_formatter('sent') }},
	delivered,
	opened,
	clicked,
    CAST(NULL AS timestamp) AS sms_responses
FROM
	{{ ref('stg_cio_messages') }} scm
where medium = 'email'
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
    sent,
    delivered,
    opened,
    clicked,
    sms_responses,
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
ON email.campaign_id=cast(rc.campaign_id AS bigint) --AND lower(sms_received.role)=lower(rc.audience)
)
SELECT * FROM add_gs_sheet
