{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}}
WITH messages AS(
SELECT 
    m.*,
    c.name AS name,
    c.tags,
	c.type AS campaign_type,
	c.state,
	c.active,
	c.actions,
	c.campaign_created,
	c.campaign_updated,
	c.timezone,
	c.frequency,
    CASE WHEN (pup.first_name IS NOT NULL AND pup.last_name IS NOT NULL) THEN 
        CONCAT(pup.first_name, ' ', pup.last_name) ELSE NULL END AS customer_name,
	c.event_name,
	c.start_hour,
	c.campaign_first_started,
	c.start_minutes,
	c.date_attribute,
	c.filter_segment_ids,
	c.trigger_segment_ids,
	c.use_customer_timezone,
    lb.bank_name,
    pup.first_name,
    pup.last_name,
    pup.email as profile_email,
    pup.phone profile_phone,
    pup.id AS pup_id,
    leads.id AS lead_id,
    leads.bank_id,
    pur.role
FROM
    {{ ref('base_cio_messages') }} m
LEFT JOIN {{ ref('base_cio_campaigns') }} c
ON m.campaign_id=c.id
LEFT JOIN {{ ref('partner_user_profiles') }} pup
ON m.customer_id=cast(pup.aggregate_id AS TEXT)
LEFT JOIN {{ ref('leads') }} leads
on m.customer_id=cast(leads.aggregate_id AS TEXT)
LEFT JOIN {{ ref('stg_lead_banks')}} lb
on pup.partner_id=lb.bank_id
LEFT JOIN {{ ref('partner_user_roles') }} pur
on pup.id=pur.user_profile_id 
)
SELECT * FROM messages
{% if is_incremental() %}
WHERE {{ local_convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}