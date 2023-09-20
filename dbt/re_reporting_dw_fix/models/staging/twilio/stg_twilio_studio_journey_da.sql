-- Complete data: flow -> FWc3e0f23a2920ad57b5e3f5c33b9de8c4
with main_cte as (
	select 
		 'HS' as warehouse
		,'subject' as event_subject
		,null as event_priority
		,'[]'::text as event_tags
		,'response' as event_response
		,'Direct Assignment' as customer_journey
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
	where rtc.flow_sid = 'FWc3e0f23a2920ad57b5e3f5c33b9de8c4')

-- Filter data: Steps ******************************************************
, raw_flow_cte as (
	select
		{% if target.type == 'snowflake' %}
		 data.context:flow.variables.userId::VARCHAR as userId
		,data.context:flow.variables.profileId::VARCHAR as profileId
		,data.context:flow.variables.aggregateId::VARCHAR as aggregateId
		,data.context:flow.variables.workerRole::VARCHAR as workerRole
		,data.context:flow.variables.buyLeadId::VARCHAR as buyLeadId
		,data.context:flow.variables.sellLeadId::VARCHAR as sellLeadId
		,data.context:flow.variables.partnerName::VARCHAR as partnerName
		,data.context:flow.variables.programName:VARCHAR as programName
		,data.context:flow.variables.timeframe::VARCHAR as timeframe
		,data.context:flow.variables.comments::VARCHAR as comments
		,data.context:flow.variables.callDuration::VARCHAR as callDuration
		,data.context:flow.variables.buyTransaction::VARCHAR as buyTransaction
		,data.context:flow.variables.sellTransaction::VARCHAR as sellTransaction
		{% else %}
		 data.context->'flow'->'variables'->>'userId' as userId
		,data.context->'flow'->'variables'->>'profileId' as profileId
		,data.context->'flow'->'variables'->>'aggregateId' as aggregateId
		,data.context->'flow'->'variables'->>'workerRole' as workerRole
		,data.context->'flow'->'variables'->>'buyLeadId' as buyLeadId
		,data.context->'flow'->'variables'->>'sellLeadId' as sellLeadId
		,data.context->'flow'->'variables'->>'partnerName' as partnerName
		,data.context->'flow'->'variables'->>'partnerProgramName' as programName
		,data.context->'flow'->'variables'->>'timeframe' as timeframe
		,data.context->'flow'->'variables'->>'comments' as comments
		,data.context->'flow'->'variables'->>'callDuration' as callDuration
		,data.context->'flow'->'variables'->>'buyTransaction' as buyTransaction
		,data.context->'flow'->'variables'->>'sellTransaction' as sellTransaction
		{% endif %}
		,data.execution_sid
		,data.context
	from main_cte data)

-- Filter data: Incoming Messages
, incoming_sms_cte as (
	select
		 main.userId
		,main.profileId
		,main.aggregateId
		,main.workerRole
		,case 
			when main.buyLeadId is null 
				then main.sellLeadId
			else main.buyLeadId end as lead_id
		,main.partnerName as partner_name
		,main.programName as program_name
		,main.timeframe
		,main.comments
		,main.callDuration
		,main.execution_sid
		,case
			when main.buyTransaction = 'true'
				then 'BUY'
			else 'SELL' end as transactionType
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
		 data.userId
		,data.profileId
		,data.aggregateId
		,data.workerRole
		,data.lead_id
		,data.partner_name
		,data.program_name
		,data.timeframe
		,data.comments
		,data.callDuration
		,data.execution_sid
		,data.transactionType
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
		left join {{ source('public', 'raw_hs_messages') }}  rhm 
			on data.sms_sid = rhm.sid)
			
-- Filter data: Raw Steps Data
, raw_flow_sms_cte as (
	select
		 main.execution_sid
		,json_data2.key as flow_step
		,json_data3.key as event_type
		{% if target.type == 'snowflake' %}
		,case 
			when json_data3.value:Sid::VARCHAR is null
				then json_data3.value:SmsSid::VARCHAR
			else json_data3.value:Sid::VARCHAR end as sms_sid
		{% else %}
		,case 
			when json_data3.value->>'Sid' is null
				then json_data3.value->>'SmsSid'
			else json_data3.value->>'Sid' end as sms_sid
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
		 main.userId
		,main.profileId
		,main.aggregateId
		,main.workerRole
		,case 
			when main.buyLeadId is null 
				then main.sellLeadId
			else main.buyLeadId end as lead_id
		,main.partnerName as partner_name
		,main.programName as program_name
		,main.timeframe
		,main.comments
		,main.callDuration
		,main.execution_sid
		,case
			when main.buyTransaction = 'true'
				then 'BUY'
			else 'SELL' end as transactionType
		,null as subflow
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
		left join {{ source('public', 'raw_hs_messages') }}  rhm 
			on data.sms_sid = rhm.sid)

-- SMS Complete DATA
, complete_sms_data as (
	select 
		 t1.userId
		,t1.profileId
		,t1.aggregateId
		,t1.workerRole
		,t1.lead_id
		,t1.partner_name
		,t1.program_name
		,t1.timeframe
		,t1.comments
		,t1.callDuration
		,t1.transactionType
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
		 t2.userId
		,t2.profileId
		,t2.aggregateId
		,t2.workerRole
		,t2.lead_id
		,t2.partner_name
		,t2.program_name
		,t2.timeframe
		,t2.comments
		,t2.callDuration
		,t2.transactionType
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
	from raw_flow_sms_final_cte t2)

, data_result_cte as (
	select 
		sms_cte.userId
		,sms_cte.profileId
		,sms_cte.aggregateId
		,sms_cte.workerRole
		,case when sms_cte.lead_id = '' then sms_cte.userid else null end as profile_aggregate_id
		,cast(nullif(sms_cte.lead_id,'') as int) lead_id
		,sms_cte.partner_name
		,sms_cte.program_name
		,sms_cte.timeframe
		,sms_cte.comments
		,sms_cte.callDuration
		,sms_cte.transactionType
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
	 drc.userId
	,drc.profileId
	,drc.aggregateId
	,drc.workerRole
	,case when drc.lead_id is null then ca.lead_id else drc.lead_id end as lead_id
	,drc.partner_name
	,drc.program_name
	,drc.timeframe
	,drc.comments
	,drc.callDuration
	,drc.transactionType
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
