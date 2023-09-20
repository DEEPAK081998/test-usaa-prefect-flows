{{
  config(
	materialized = 'table'
	)
}}
with 

zendesk_info  as (
select
	lead_id,
	 warehouse,
	join_field,
	'Zendesk' as event_source,
	event_medium,
	event_type,
	system_event_timestamp,
	event_timestamp,
	event_subject,
	event_content,
	event_priority,
	a.event_tags::text as event_tags,
	event_source_url,
	event_response,
	customer_journey,
	a.related_events::text as related_events,
	a.event_status
from {{ ref("stg_zendesk_clean") }} a
)

, twilio_info AS (
	SELECT
	   stg.lead_id::text as lead_id,
	   'HS' AS Warehouse,
	   stg.pup_id::text AS join_field,
	   'Twillio' as event_source,
	   --sid as source_id,
	   'sms' as event_medium,
	   case direction when 'inbound' then 'inbound' else 'outbound' end as event_type,
	   --date_sent ,
	   {% if target.type == 'snowflake' %}
	   CONVERT_TIMEZONE('UTC', date_sent) as system_event_timestamp,
	   CONVERT_TIMEZONE('America/Chicago', date_sent) as event_timestamp,
	   {% else %}
	   date_sent at time zone 'UTC' as system_event_timestamp,
	   date_sent at time zone 'CST' as event_timestamp,
	   {% endif %}
	   'subject' as event_subject,
	   --'response' as event_response,
	   body as event_content,
	   null as event_priority,
	   '[]'::text as event_tags,
	   uri as event_source_url,
	   'response' as event_response,
	   'support' as customer_journey,
	   '[]'::text as related_events,
	   status as event_status
	FROM {{ ref('base_twilio_messages') }}  
	left join {{ ref('stg_link_twilio_pup_lead_client') }} stg
	on base_twilio_messages."to" = stg.twilio_phone_id
)
,  cio_prod_activities as (
	select *,row_number() over(partition by delivery_id order by {{  special_column_name_formatter('timestamp') }} desc) as dedup from {{ source('public', 'cio_prod_activities') }} cpa
	where cpa.delivery_type  = 'email' and type not in ('drafted_email')
)

, cio_prod_message as (
	select 
		cpm.id as delivery_id,
		cpm.subject as event_content,
		cpc.name as event_subject,
		CASE
			WHEN cpc.id IN (23,24,14,21,20,11,18,26,2,25,44) THEN 'Verified to Pending'
			WHEN cpc.id IN (39,31,37,36,42) THEN 'Close to Reward'
			WHEN cpc.id = 44 THEN 'Enroll to Verify'
			ELSE NULL
		END AS customer_journey
	from {{ source('public', 'cio_prod_messages') }} cpm 
	left join {{ source('public', 'cio_prod_campaigns') }} cpc
	on cpm.campaign_id = cpc.id
)
, cio_int_cte as (
	select 
	--null::text as lead_id,
	'HS' as warehouse,
	customer_id as join_field,
	'Customer.IO' as event_source,
	delivery_type  as event_medium,
	'inbound'  as event_type,
	{% if target.type == 'snowflake' %}
	case
			when data:delivered::VARCHAR is null then CONVERT_TIMEZONE('UTC', cio_prod_activities.timestamp::VARCHAR)
			else CONVERT_TIMEZONE('UTC', data:delivered::VARCHAR)
	end as system_event_timestamp,
	case
			when data:delivered::VARCHAR is null then CONVERT_TIMEZONE('America/Chicago', cio_prod_activities.timestamp::VARCHAR)
			else CONVERT_TIMEZONE('America/Chicago', data:delivered::VARCHAR)
	end as event_timestamp,
	{% else %}
	case 
			when data->>'delivered' is null then to_timestamp(cio_prod_activities."timestamp") at time zone 'UTC'
			else to_timestamp((data->>'delivered')::bigint)  at time zone 'UTC'
	end as system_event_timestamp,
	case 
			when data->>'delivered' is null then to_timestamp(cio_prod_activities."timestamp") at time zone 'CST'
			else to_timestamp((data->>'delivered')::bigint)  at time zone 'CST'
	end as event_timestamp,
	{% endif %}
	cio_prod_message.event_subject,
	cio_prod_message.event_content,
	'' as event_priority,
	'[]'::text as event_tags,
	'' as event_source_url,
	cio_prod_activities.type as event_response,
	cio_prod_message.customer_journey as customer_journey,
	'[]'::text as related_events,
	cio_prod_activities.type as event_status
	--customer_identifiers->>'cio_id' as cio_id,
	--prod_activities.delivery_id as id
	from cio_prod_activities
	left join cio_prod_message
	on cio_prod_activities.delivery_id = cio_prod_message.delivery_id
	where dedup =1
)

, customer_io_info AS (
	SELECT 
		distinct
		ca.lead_id::text as lead_id,
		cte.*
	FROM cio_int_cte cte 
	left join {{ ref('current_assignments') }} ca
	on cte.join_field::{{ uuid_formatter() }} = ca.profile_aggregate_id
)

, customer_io_sms_info as (
	SELECT
		lead_id::text as lead_id,'HS' as warehouse, aggregate_id as join_field, 'Customer.IO' as event_source,'sms' as event_medium,'inbound' as event_type,system_date ,reporting_date
		,campaign_name  as event_subject
		,questionresponse  as event_content
		,null as event_priority
		,'[]' as event_tags
		,null as event_source_url
		,response  as event_response
		,bccsr.customer_journey
		,'[]' as related_events
		,case when bccsr.response is not null then 'responded' else null end as event_status 
	FROM {{ ref('base_cio_campaign_sms_response') }} bccsr 
)
, final_cte AS(

	SELECT * FROM zendesk_info
	UNION ALL
	SELECT * FROM twilio_info
	UNION ALL
	SELECT * FROM customer_io_info
	UNION ALL 
	SELECT * FROM customer_io_sms_info

)
select * from final_cte f 