
with messages AS(
select 
	id,
	type,
	to_timestamp(created) as created,

	{% if target.type == 'snowflake' %}
	to_timestamp(cast(metrics:sent as int)) as sent,
	to_timestamp(cast(metrics:opened as int)) as opened,
	to_timestamp(cast(metrics:delivered as int)) as delivered,
	to_timestamp(cast(metrics:clicked as int)) as clicked,
	to_timestamp(cast(metrics:"link:10" as int)) as link,
	customer_identifiers:id::VARCHAR as customer_id,
	customer_identifiers:email::VARCHAR as customer_email,
	customer_identifiers:cio_id::VARCHAR as cio_id,
	{% else %}
	to_timestamp(cast(metrics->'sent' as int)) as sent, 
	to_timestamp(cast(metrics->'opened' as int)) as opened, 
	to_timestamp(cast(metrics->'delivered' as int)) as delivered,
	to_timestamp(cast(metrics->'clicked' as int)) as clicked,
	to_timestamp(cast(metrics->'link:10' as int)) as link,
	customer_identifiers->>'id' as customer_id,
	customer_identifiers->>'email' as customer_email,
	customer_identifiers->>'cio_id' as cio_id,
	{% endif %}
	subject,
	action_id,
	forgotten,
	recipient,
	content_id,
	campaign_id,
	broadcast_id,
	newsletter_id,
	deduplicate_id,
	failure_message,
	msg_template_id,
	parent_action_id,
	case when type='email' then 'email' when type='webhook' then 'sms' end as medium,
	transactional_message_id 
from {{ source('public', 'cio_prod_messages') }}    
)
SELECT * from messages
