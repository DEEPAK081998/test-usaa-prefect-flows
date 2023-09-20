{{
  config(
    materialized = 'table',
    )
}}
With custom_fields as (
	select rztcf.{{  special_column_name_formatter('_airbyte_raw_zendesk_tickets_hashid') }},rztcf.{{  special_column_name_formatter('_airbyte_ab_id') }},rztf.id ,rztf.title,rztcf.value from {{ source('public', 'raw_zendesk_tickets_custom_fields') }} rztcf
	left join {{ source('public', 'raw_zendesk_ticket_fields') }} rztf 
	on rztcf.id = rztf.id
)
,chase_cte as (
	select rzt.{{  special_column_name_formatter('_airbyte_unique_key') }}  as chase_id from {{ source('public', 'raw_zendesk_tickets') }} rzt
	inner join custom_fields 
	on rzt.{{  special_column_name_formatter('_airbyte_raw_zendesk_tickets_hashid') }}  = custom_fields.{{  special_column_name_formatter('_airbyte_raw_zendesk_tickets_hashid') }}
	and rzt.{{  special_column_name_formatter('_airbyte_ab_id') }} = custom_fields.{{  special_column_name_formatter('_airbyte_ab_id') }}
	where custom_fields.title = 'Program' and lower(custom_fields.value) = 'chase'
)
, cte_d as (
    select 
        rzt.{{  special_column_name_formatter('_airbyte_unique_key') }}::text as new_id,
        rzt.id as source_id,
        {% if target.type == 'snowflake' %}
        rzt.via:channel::VARCHAR as event_medium,
        'inbound' as event_type, -- to validate
        CONVERT_TIMEZONE('UTC', rzt.generated_timestamp::VARCHAR) as system_event_timestamp,
        CONVERT_TIMEZONE('America/Chicago', rzt.generated_timestamp::VARCHAR) as event_timestamp,
        rzt.raw_subject as event_subject,
        rzt.description  as event_content,
        rzt.priority  as event_priority,
        rzt.tags as event_tags,
        rzt.url as event_source_url,
        rzt.satisfaction_rating:score::VARCHAR as event_response,
        {% else %}
        rzt.via->>'channel' as event_medium,
        'inbound' as event_type, -- to validate 
        to_timestamp(rzt.generated_timestamp) at time zone 'UTC' as system_event_timestamp,
        to_timestamp(rzt.generated_timestamp) at time zone 'CST' as event_timestamp, 
        rzt.raw_subject as event_subject, 
        rzt.description  as event_content,
        rzt.priority  as event_priority,
        rzt.tags as event_tags,
        rzt.url as event_source_url,
        rzt.satisfaction_rating->>'score' as event_response,
        {% endif %}
        'support' as customer_journey,
        rzt.followup_ids as related_events,
        rzt.status as event_status,
        custom_fields.title,
        custom_fields.value
    from {{ source('public', 'raw_zendesk_tickets') }} rzt
    left join custom_fields 
    on rzt.{{  special_column_name_formatter('_airbyte_raw_zendesk_tickets_hashid') }}  = custom_fields.{{  special_column_name_formatter('_airbyte_raw_zendesk_tickets_hashid') }}
    and rzt.{{  special_column_name_formatter('_airbyte_ab_id') }} = custom_fields.{{  special_column_name_formatter('_airbyte_ab_id') }}
    where rzt.{{  special_column_name_formatter('_airbyte_unique_key') }}  not in (
        select chase_id from chase_cte
        )
)
select 
	new_id as airbyte_id,
	source_id,
	event_medium, 
	event_type, -- to validate 
	system_event_timestamp,
	event_timestamp, 
	event_subject, 
	event_content,
	event_priority,
	event_tags,
	event_source_url,
	event_response,
	customer_journey,
	related_events,
	event_status,
	MAX(case title when 'Lead ID' then value end ) as lead_id,
	MAX(case  when title= 'Agent Profile URL' then reverse(split_part(reverse(value),'/',1)) end ) as join_field,
    MAX(case  when title ='Agent Profile URL' and  split_part(split_part((value),'/',3),'.',1) = 'admin' then 'HS'	 else 'no-defined'		end) as warehouse
	from cte_d
--where new_id = '1fcb5c4c068ab273a14f94d2bf31a665'
group by new_id,
	source_id,
	event_medium, 
	event_type, 
	system_event_timestamp,
	event_timestamp, 
	event_subject, 
	event_content,
	event_priority,
	event_tags,
	event_source_url,
	event_response,
	customer_journey,
	related_events,
	event_status