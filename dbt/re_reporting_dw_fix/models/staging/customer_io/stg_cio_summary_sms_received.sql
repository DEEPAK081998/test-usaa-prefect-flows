with sms_received AS(
    select
        cust_cio_id as cio_id,
        customer_id as aggregate_id,
        {{  special_column_name_formatter('role') }},
        CASE WHEN campaign = 'Incoming Assignment Handler (10)' THEN 10
        WHEN campaign = '7-14 Day LO Sentiment (14)' THEN 14
        WHEN campaign = 'Customer LO Sentiment' THEN 14
        WHEN campaign = 'Agent LO Sentiment' THEN 14
        WHEN campaign = '3WayIntro' THEN 16
        WHEN campaign = '3-Way Intro' THEN 14
        WHEN campaign = 'Inactive Outreach v2 (18)' THEN 18
        WHEN campaign = 'Customer - Verified Connection Outreach (20)' THEN 20
        WHEN campaign = 'Verified Connection - Customer' THEN 20
        WHEN campaign = 'Verified Connection - Agent' THEN 21
        WHEN campaign = '30-Day Check-In' THEN 24
        WHEN campaign = '45-Day Check-In' THEN 25
        WHEN campaign = 'Recurring 45 Day Customer Outreach (25)' THEN 25
        ELSE campaign_id end as campaign_id,
        {{  special_column_name_formatter('name') }},
        campaign as campaign_type,
        'sms' as medium,
        bank_name,
        customer_name,
        CAST(NULL AS timestamp) AS sent,
        CAST(NULL AS timestamp) AS delivered,
        CAST(NULL AS timestamp) AS opened,
        CAST(NULL AS timestamp) AS clicked,
        timestamp AS sms_responses
    from 
        {{ ref('stg_cio_activities') }}
    where
        {{  special_column_name_formatter('name') }}='outreach'
),
add_gs_sheet AS(
SELECT
    cio_id,
    cast(aggregate_id as {{ uuid_formatter() }}) as aggregate_id,
    role,
    sms_received.campaign_id,
    name,
    campaign_type,
    null AS recipient,
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
    sms_received
LEFT JOIN {{ source('public', 'raw_campaigns') }} rc
ON sms_received.campaign_id=cast(rc.campaign_id AS bigint) AND lower(sms_received.role)=lower(rc.audience)
)
SELECT * FROM add_gs_sheet