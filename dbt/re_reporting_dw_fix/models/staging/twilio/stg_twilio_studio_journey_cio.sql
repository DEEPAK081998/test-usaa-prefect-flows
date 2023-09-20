-- Model flows: ['FWe00125df2d95fd38949df533b2cc1634', 'FW7153d4d651a8b6d1be5eb100d770aaeb']
with main_cte as (
	select 
		 'HS' as warehouse
		,'subject' as event_subject
		,null as event_priority
		,'[]'::text as event_tags
		,'response' as event_response
		,'CIO Campaigns' as customer_journey
		,'[]'::text as related_events
		,rtc.execution_sid
		,rtc.context
		{% if target.type == 'snowflake' %}
		,case 
			when rtc.context:flow.trigger.message::VARCHAR is null 
				then case
						when rtc.context:flow.trigger.call::VARCHAR is null
							then 'API'
						else 'CALL' end
				else 'SMS' end as "trigger"
		{% else %}
		,case 
			when rtc.context->'trigger'->>'message' is null 
				then case
						when rtc.context->'trigger'->>'call' is null
							then 'API'
						else 'CALL' end
				else 'SMS' end as trigger
		{% endif %}
	from {{ ref('stg_twiliostudio_execution_context') }} rtc 
	where rtc.flow_sid in ('FWe00125df2d95fd38949df533b2cc1634', 'FW7153d4d651a8b6d1be5eb100d770aaeb'))

-- Filter data: Subflows ***************************************************
, subflows_cte as (
	select
		{% if target.type == 'snowflake' %}
		 data.context:flow.data.id::VARCHAR as id
		,data.context:flow.data.cio_id::VARCHAR as cio_id
		,data.context:flow.data.lead_id::VARCHAR as lead_id
		,data.context:flow.data.leadId::VARCHAR as leadId
		,case 
			when data.context:flow.data.buy_lead_id::VARCHAR is null
				then case 
						when data.context:flow.data.buyLeadId::VARCHAR is null
							then data.context:flow.data.sellLeadId
						else data.context:flow.data.buyLeadId::VARCHAR end
			else data.context:flow.data.buy_lead_id::VARCHAR
			end as buy_lead_id
		,data.context:flow.data.partnerName::VARCHAR as partnerName
		,data.context:flow.data.partner_name::VARCHAR as partner_name
		,data.context:flow.data.programName::VARCHAR as programName
		,data.context:flow.data.program_name::VARCHAR as program_name
		,data.context:flow.data.service::VARCHAR as service
		,data.context:flow.data.campaign_id::VARCHAR as campaign_id
		,data.context:flow.data.transactionType::VARCHAR as transactionType
		,data.context:flow.data.twilio_stage::VARCHAR as twilio_stage
		,data.context:flow.data.active_status::VARCHAR as active_status
		,data.context:flow.data.inactive_status::VARCHAR as inactive_status
		,data.context:flow.data.check_in_day::VARCHAR as check_in_day
		{% else %}
		 data.context->'flow'->'data'->>'id' as id
		,data.context->'flow'->'data'->>'cio_id' as cio_id
		,data.context->'flow'->'data'->>'lead_id' as lead_id
		,data.context->'flow'->'data'->>'leadId' as leadId
		,case 
			when data.context->'flow'->'data'->>'buy_lead_id' is null
				then case 
						when data.context->'flow'->'data'->>'buyLeadId' is null
							then data.context->'flow'->'data'->>'sellLeadId'
						else data.context->'flow'->'data'->>'buyLeadId' end
			else data.context->'flow'->'data'->>'buy_lead_id'
			end as buy_lead_id
		,data.context->'flow'->'data'->>'partnerName' as partnerName
		,data.context->'flow'->'data'->>'partner_name' as partner_name
		,data.context->'flow'->'data'->>'programName' as programName
		,data.context->'flow'->'data'->>'program_name' as program_name
		,data.context->'flow'->'data'->>'service' as service
		,data.context->'flow'->'data'->>'campaign_id' as campaign_id
		,data.context->'flow'->'data'->>'transactionType' as transactionType
		,data.context->'flow'->'data'->>'twilio_stage' as twilio_stage
		,data.context->'flow'->'data'->>'active_status' as active_status
		,data.context->'flow'->'data'->>'inactive_status' as inactive_status
		,data.context->'flow'->'data'->>'check_in_day' as check_in_day
		{% endif %}
		,data.execution_sid
		,data.context
		,stsc.context step_context
	from main_cte data 
		left join {{ ref('stg_twiliostudio_step') }} sts 
			on data.execution_sid = sts.execution_sid
		left join {{ ref('stg_twiliostudio_step_context') }}  stsc 
			on sts.sid = stsc.step_sid 
	where sts.transitioned_to like '%/Ended')
	
-- Filter data: Steps ******************************************************
, raw_flow_cte as (
	select
		{% if target.type == 'snowflake' %}
		 data.context:flow.data.id::VARCHAR as id
		,data.context:flow.data.cio_id::VARCHAR as cio_id
		,data.context:flow.data.lead_id::VARCHAR as lead_id
		,data.context:flow.data.leadId::VARCHAR as leadId
		,case 
			when data.context:flow.data.buy_lead_id::VARCHAR is null
				then case 
						when data.context:flow.data.buyLeadId::VARCHAR is null
							then data.context:flow.data.sellLeadId
						else data.context:flow.data.buyLeadId::VARCHAR end
			else data.context:flow.data.buy_lead_id::VARCHAR
			end as buy_lead_id
		,data.context:flow.data.partnerName::VARCHAR as partnerName
		,data.context:flow.data.partner_name::VARCHAR as partner_name
		,data.context:flow.data.programName::VARCHAR as programName
		,data.context:flow.data.program_name::VARCHAR as program_name
		,data.context:flow.data.service::VARCHAR as service
		,data.context:flow.data.campaign_id::VARCHAR as campaign_id
		,data.context:flow.data.transactionType::VARCHAR as transactionType
		,data.context:flow.data.twilio_stage::VARCHAR as twilio_stage
		,data.context:flow.data.active_status::VARCHAR as active_status
		,data.context:flow.data.inactive_status::VARCHAR as inactive_status
		,data.context:flow.data.check_in_day::VARCHAR as check_in_day
		{% else %}
		 data.context->'flow'->'data'->>'id' as id
		,data.context->'flow'->'data'->>'cio_id' as cio_id
		,data.context->'flow'->'data'->>'lead_id' as lead_id
		,data.context->'flow'->'data'->>'leadId' as leadId
		,case 
			when data.context->'flow'->'data'->>'buy_lead_id' is null
				then case 
						when data.context->'flow'->'data'->>'buyLeadId' is null
							then data.context->'flow'->'data'->>'sellLeadId'
						else data.context->'flow'->'data'->>'buyLeadId' end
			else data.context->'flow'->'data'->>'buy_lead_id'
			end as buy_lead_id
		,data.context->'flow'->'data'->>'partnerName' as partnerName
		,data.context->'flow'->'data'->>'partner_name' as partner_name
		,data.context->'flow'->'data'->>'programName' as programName
		,data.context->'flow'->'data'->>'program_name' as program_name
		,data.context->'flow'->'data'->>'service' as service
		,data.context->'flow'->'data'->>'campaign_id' as campaign_id
		,data.context->'flow'->'data'->>'transactionType' as transactionType
		,data.context->'flow'->'data'->>'twilio_stage' as twilio_stage
		,data.context->'flow'->'data'->>'active_status' as active_status
		,data.context->'flow'->'data'->>'inactive_status' as inactive_status
		,data.context->'flow'->'data'->>'check_in_day' as check_in_day
		{% endif %}
		,data.execution_sid
		,data.context
		,stsc.context step_context
	from main_cte data 
		left join {{ ref('stg_twiliostudio_step') }} sts 
			on data.execution_sid = sts.execution_sid
		left join {{ ref('stg_twiliostudio_step_context') }} stsc 
			on sts.sid = stsc.step_sid 
	where sts.transitioned_to = 'Ended' and sts.transitioned_from not like '%/Ended')
	
-- Filter data: Incoming Messages
, incoming_sms_cte as (
	select
		 main.id
		,main.cio_id
		,case 
			when main.lead_id is null 
				then case 
						when main.leadid is null 
							then main.buy_lead_id 
						else main.leadid end 
			else main.lead_id end as lead_id
		,case
			when main.partner_name is null
				then main.partnername
			else main.partner_name end as partner_name
		,case
			when main.program_name is null
				then main.programname
			else main.program_name end as program_name
		,main.service
		,main.campaign_id
		,main.execution_sid
		,main.transactionType
		,main.twilio_stage
		,main.active_status
		,main.inactive_status
		,main.check_in_day
		,null as subflow
		,null as flow_step
		,'inbound' as event_type
		{% if target.type == 'snowflake' %}
		,case 
			when json_data.value:message.SmsSid::VARCHAR is null
				then json_data.value:message.Sid::VARCHAR
			else json_data.value:message.SmsSid::VARCHAR end as sms_sid
		{% else %}
		,case 
			when json_data.value->'message'->>'SmsSid' is null 
				then json_data.value->'message'->>'Sid' 
			else json_data.value->'message'->>'SmsSid' end as sms_sid
		{% endif %}
		,'sms' as event_medium
	from raw_flow_cte as main,
		{% if target.type == 'snowflake' %}
		lateral flatten(input => parse_json(main.context)) as json_data
	where json_data.value:message::VARCHAR is not null
		{% else %}
		jsonb_each(main.context) as json_data(key, value)
	where json_data.value->>'message' is not null 
		{% endif %})

-- Filter data: Incoming Messages SMS Joined Data
, incoming_sms_final_cte as (
	select 
		 data.id
		,data.cio_id
		,data.lead_id
		,data.partner_name
		,data.program_name
		,data.service
		,data.campaign_id
		,data.transactionType
		,data.twilio_stage
		,data.active_status
		,data.inactive_status
		,data.check_in_day
		,data.execution_sid
		,data.subflow
		,data.flow_step
		,data.event_type
		,data.sms_sid
		,data.event_medium
		,rhm.body as raw_event_content
		,rhm.body as normalize_event_content
		,rhm.uri as event_source_url
		,rhm.date_sent
		,rhm.status
		,rhm.error_code
		,rhm.error_message 
		,rhm.num_segments
		,rhm."to"
		,rhm."from"
	from incoming_sms_cte as data
		left join {{ source('public', 'raw_hs_messages') }} rhm 
			on data.sms_sid = rhm.sid)
	
-- Filter data: Subflows Level 1
, subflows_data_cte as (
	select
		 data.execution_sid
		,json_data2.key as subflow
		,json_data2.value as json_data
	from subflows_cte as data,
		{% if target.type == 'snowflake' %}
		lateral flatten(input => parse_json(data.step_context)) as json_data,
		lateral flatten(input => parse_json(json_data.value)) as json_data2
	where json_data2.value:widgets::VARCHAR  is not null
		{% else %}
		jsonb_each(data.step_context) as json_data(key, value),
		jsonb_each(json_data.value) as json_data2(key, value)
	where json_data2.value->>'widgets' is not null
		{% endif %})

-- Filter data: Subflows Level 2
, subflows_filter_cte as (
	select 
		 data.execution_sid
		,data.subflow
		,json_data2.key as flow_step
		,json_data2.value as json_data
	from subflows_data_cte as data,
		{% if target.type == 'snowflake' %}
		lateral flatten(input => parse_json(data.json_data)) as json_data,
		lateral flatten(input => parse_json(json_data.value)) as json_data2
	where json_data2.value:inbound::VARCHAR is not null or json_data2.value:outbound::VARCHAR is not null
		{% else %}
		jsonb_each(data.json_data) as json_data(key, value),
		jsonb_each(json_data.value) as json_data2(key, value)
	where json_data2.value->>'inbound' is not null or json_data2.value->>'outbound' is not null
		{% endif %})

-- Filter data: Subflows Normalized Response
, subflows_normalized_response_cte as (
	select 
		 data.execution_sid
		{% if target.type == 'snowflake' %}
		,json_data2.value:rawResponse::VARCHAR as rawResponse
		,json_data2.value:normalizedResponse::VARCHAR as normalizedResponse
		 {% else %}
		,json_data2.value->>'rawResponse' as rawResponse
		,json_data2.value->>'normalizedResponse' as normalizedResponse
		{% endif %}
	from subflows_data_cte as data,
		{% if target.type == 'snowflake' %}
		lateral flatten(input => parse_json(data.json_data)) as json_data,
		lateral flatten(input => parse_json(json_data.value)) as json_data2
	where json_data.key = 'widgets' and json_data2.value:normalizedResponse::VARCHAR is not null
		{% else %}
		jsonb_each(data.json_data) as json_data(key, value),
		jsonb_each(json_data.value) as json_data2(key, value)
	where json_data.key = 'widgets' and json_data2.value->>'normalizedResponse' is not null
		{% endif %})

-- Filter data: Subflows SMS Data
, subflows_sms_cte as (	
	select 
		 data.execution_sid
		,data.subflow
		,data.flow_step
		,json_data.key as event_type
		{% if target.type == 'snowflake' %}
		,case 
			when json_data.value:SmsSid::VARCHAR is null
				then json_data.value:Sid::VARCHAR
			else json_data.value:SmsSid::VARCHAR end as sms_sid
		,'sms' as event_medium
		,json_data.value:Body as raw_event_content
		{% else %}
		,case 
			when json_data.value->>'SmsSid' is null 
				then json_data.value->>'Sid' 
			else json_data.value->>'SmsSid' end as sms_sid
		,'sms' as event_medium
		,json_data.value->>'Body' as raw_event_content
		{% endif %}
	from subflows_filter_cte as data,
		{% if target.type == 'snowflake' %}
		lateral flatten(input => parse_json(data.json_data)) as json_data
		{% else %}
		jsonb_each(data.json_data) as json_data(key, value)
		{% endif %}
	where json_data.key in ('inbound','outbound'))

-- Filter data: Subflows SMS Joined Data
, subflows_sms_final_cte as (	
	select 
		 main.id
		,main.cio_id
		,case 
			when main.lead_id is null 
				then case 
						when main.leadid is null 
							then main.buy_lead_id 
						else main.leadid end 
			else main.lead_id end as lead_id
		,case
			when main.partner_name is null
				then main.partnername
			else main.partner_name end as partner_name
		,case
			when main.program_name is null
				then main.programname
			else main.program_name end as program_name
		,main.service
		,main.campaign_id
		,main.transactionType
		,main.twilio_stage
		,main.active_status
		,main.inactive_status
		,main.check_in_day
		,data.execution_sid
		,data.subflow
		,data.flow_step
		,data.event_type
		,data.sms_sid
		,data.event_medium
		,data.raw_event_content
		,normalized_reponse.normalizedResponse as normalize_event_content
		,rhm.uri as event_source_url
		,rhm.date_sent
		,rhm.status
		,rhm.error_code
		,rhm.error_message 
		,rhm.num_segments
		,rhm."to"
		,rhm."from"
	from subflows_cte main
		left join subflows_sms_cte as data
			on main.execution_sid = data.execution_sid
		left join {{ source('public', 'raw_hs_messages') }} rhm 
			on data.sms_sid = rhm.sid
		left join subflows_normalized_response_cte normalized_reponse
			on data.execution_sid = normalized_reponse.execution_sid and data.raw_event_content = normalized_reponse.rawResponse)

-- Filter data: Raw Steps Data
, raw_flow_sms_cte as (
	select
		 main.execution_sid
		,null as subflow
		,json_data2.key as flow_step
		,json_data3.key as event_type
		{% if target.type == 'snowflake' %}
		,case 
			when json_data3.value:SmsSid::VARCHAR is null
				then json_data3.value:Sid::VARCHAR
			else json_data3.value:SmsSid::VARCHAR end as sms_sid
		{% else %}
		,case 
			when json_data3.value->>'SmsSid' is null 
				then json_data3.value->>'Sid' 
			else json_data3.value->>'SmsSid' end as sms_sid
		{% endif %}
		,'sms' as event_medium
	from raw_flow_cte as main,
		{% if target.type == 'snowflake' %}
		lateral flatten(input => parse_json(main.context)) as json_data,
		lateral flatten(input => parse_json(json_data.value)) as json_data2,
		lateral flatten(input => parse_json(json_data2.value)) as json_data3
		{% else %}
		jsonb_each(main.context) as json_data(key, value),
		jsonb_each(json_data.value) as json_data2(key, value),
		jsonb_each(json_data2.value) as json_data3(key, value)
		{% endif %}
	where json_data.key = 'widgets' 
		and (json_data3.key = 'inbound' or json_data3.key = 'outbound'))

-- Filter data: Raw Steps SMS Joined Data
, raw_flow_sms_final_cte as (	
	select 
		 main.id
		,main.cio_id
		,case 
			when main.lead_id is null 
				then case 
						when main.leadid is null 
							then main.buy_lead_id 
						else main.leadid end 
			else main.lead_id end as lead_id
		,case
			when main.partner_name is null
				then main.partnername
			else main.partner_name end as partner_name
		,case
			when main.program_name is null
				then main.programname
			else main.program_name end as program_name
		,main.service
		,main.campaign_id
		,main.execution_sid
		,main.transactionType
		,main.twilio_stage
		,main.active_status
		,main.inactive_status
		,main.check_in_day
		,data.subflow
		,data.flow_step
		,data.event_type
		,data.sms_sid
		,data.event_medium
		,rhm.body as raw_event_content
		,rhm.body as normalize_event_content
		,rhm.uri as event_source_url
		,rhm.date_sent
		,rhm.status
		,rhm.error_code
		,rhm.error_message 
		,rhm.num_segments
		,rhm."to"
		,rhm."from"
	from raw_flow_cte main
		left join raw_flow_sms_cte as data
			on main.execution_sid = data.execution_sid
		left join {{ source('public', 'raw_hs_messages') }} rhm 
			on data.sms_sid = rhm.sid)

-- SMS Complete DATA
, complete_sms_data as (
	select 
		 t1.id
		,t1.cio_id
		,t1.lead_id
		,t1.partner_name
		,t1.program_name
		,t1.service
		,t1.campaign_id
		,t1.transactionType
		,t1.twilio_stage
		,t1.active_status
		,t1.inactive_status
		,t1.check_in_day
		,t1.execution_sid
		,t1.subflow
		,t1.flow_step
		,t1.event_type
		,t1.sms_sid
		,t1.event_medium
		,t1.raw_event_content
		,t1.normalize_event_content
		,t1.event_source_url
		,t1.date_sent
		,t1.status
		,t1.error_code
		,t1.error_message 
		,t1.num_segments
		,t1."to"
		,t1."from"
	from incoming_sms_final_cte t1
	union all
	select
		 t2.id
		,t2.cio_id
		,t2.lead_id
		,t2.partner_name
		,t2.program_name
		,t2.service
		,t2.campaign_id
		,t2.transactionType
		,t2.twilio_stage
		,t2.active_status
		,t2.inactive_status
		,t2.check_in_day
		,t2.execution_sid
		,t2.subflow
		,t2.flow_step
		,t2.event_type
		,t2.sms_sid
		,t2.event_medium
		,t2.raw_event_content
		,t2.normalize_event_content
		,t2.event_source_url
		,t2.date_sent
		,t2.status
		,t2.error_code
		,t2.error_message 
		,t2.num_segments
		,t2."to"
		,t2."from"
	from subflows_sms_final_cte t2
	union all
	select 
		 t3.id
		,t3.cio_id
		,t3.lead_id
		,t3.partner_name
		,t3.program_name
		,t3.service
		,t3.campaign_id
		,t3.transactionType
		,t3.twilio_stage
		,t3.active_status
		,t3.inactive_status
		,t3.check_in_day
		,t3.execution_sid
		,t3.subflow
		,t3.flow_step
		,t3.event_type
		,t3.sms_sid
		,t3.event_medium
		,t3.raw_event_content
		,t3.normalize_event_content
		,t3.event_source_url
		,t3.date_sent
		,t3.status
		,t3.error_code
		,t3.error_message 
		,t3.num_segments
		,t3."to"
		,t3."from" 
	from raw_flow_sms_final_cte t3)

, data_result_cte as (
	select 
		sms_cte.id
		,sms_cte.cio_id
		,case when sms_cte.lead_id like '%-%' then sms_cte.lead_id else null end as profile_aggregate_id
		,cast(nullif(case when sms_cte.lead_id like '%-%' then null else sms_cte.lead_id end,'') as int) as lead_id
		,sms_cte.partner_name
		,sms_cte.program_name
		,sms_cte.service
		,sms_cte.campaign_id
		,sms_cte.transactionType
		,sms_cte.twilio_stage
		,sms_cte.active_status
		,sms_cte.inactive_status
		,sms_cte.check_in_day
		,main_cte.warehouse
		,null as join_field
		,'Twilio' as event_source
		,sms_cte.event_medium
		,sms_cte.event_type
		{% if target.type == 'snowflake' %}
			,CONVERT_TIMEZONE('UTC', sms_cte.date_sent) as system_event_timestamp
			,CONVERT_TIMEZONE('America/Chicago', sms_cte.date_sent) as event_timestamp
		{% else %}
			,sms_cte.date_sent at time zone 'UTC' as system_event_timestamp
			,sms_cte.date_sent at time zone 'CST' as event_timestamp
		{% endif %}
		,main_cte.event_subject
		,sms_cte.raw_event_content
		,sms_cte.normalize_event_content
		,main_cte.event_priority
		,main_cte.event_tags
		,sms_cte.event_source_url
		,main_cte.event_response
		,main_cte.customer_journey 
		,sms_cte.subflow
		,sms_cte.flow_step
		,main_cte."trigger"
		,main_cte.related_events
		,sms_cte.status
		,sms_cte.error_code
		,sms_cte.error_message
		,sms_cte.num_segments
		,sms_cte."to"
		,sms_cte."from"
		,main_cte.execution_sid
	from main_cte
		left join complete_sms_data sms_cte 
			on main_cte.execution_sid = sms_cte.execution_sid)


-- RESULT
select
	 drc.id
	,drc.cio_id
	,case when drc.lead_id is null then ca.lead_id else drc.lead_id end as lead_id
	,drc.partner_name
	,drc.program_name
	,drc.service
	,drc.campaign_id
	,drc.transactionType
	,drc.twilio_stage
	,drc.active_status
	,drc.inactive_status
	,drc.check_in_day
	,drc.warehouse
	,drc.join_field
	,drc.event_source
	,drc.event_medium
	,drc.event_type
	,drc.system_event_timestamp
	,drc.event_timestamp
	,drc.event_subject
	,drc.raw_event_content
	,drc.normalize_event_content
	,drc.event_priority
	,drc.event_tags
	,drc.event_source_url
	,drc.event_response
	,drc.customer_journey 
	,drc.subflow
	,drc.flow_step
	,drc."trigger"
	,drc.related_events
	,drc.status
	,drc.error_code
	,drc.error_message
	,drc.num_segments
	,drc."to"
	,drc."from"
	,drc.execution_sid
from data_result_cte drc 
	left join {{ ref('current_assignments') }} ca 
		on drc.profile_aggregate_id = ca.profile_aggregate_id::VARCHAR