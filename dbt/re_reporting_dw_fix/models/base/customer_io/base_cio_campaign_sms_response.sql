{{
  config(
    materialized = 'table',
    )
}}
with source_cte as ( 
  select
  {% if target.type == 'snowflake' %}
    trim(cpad.data:campaign::VARCHAR) as campaign_name,
    CASE trim(cpad.data:campaign::VARCHAR)
  {% else %}
    trim(cpad.data->>'campaign') as campaign_name,
    CASE trim(cpad.data->>'campaign')
    {% endif %}
      WHEN 'Agent LO Sentiment' THEN 14
      WHEN '45-Day Check-In' THEN 25
      WHEN 'Verified Connection - Agent' THEN 21
      WHEN 'On Hold Outreach' THEN 26
      WHEN '30-Day Check-In' THEN 24
      WHEN 'Customer LO Sentiment' THEN 14
      WHEN 'Verified Connection - Customer' THEN 20
      WHEN '3WayIntro' THEN null
      WHEN 'Inactive Outreach' THEN 18
      {% if target.type == 'snowflake' %}
      ELSE  (cpad.data:campaignId::VARCHAR)::int
      END AS campaign_id,
    cpad.data:agent::VARCHAR as AgentObj,
    CASE trim(cpad.data:campaign::VARCHAR)
      {% else %}
      ELSE  (cpad.data->>'campaignId')::int
      END AS campaign_id,
    cpad.data->>'agent' as AgentObj,
    CASE trim(cpad.data->>'campaign')
      {% endif %}
      WHEN 'Agent LO Sentiment' THEN 'CUSTOMER'
      WHEN '45-Day Check-In' THEN 'CUSTOMER'
      WHEN 'Verified Connection - Agent' THEN 'AGENT'
      WHEN 'On Hold Outreach' THEN 'CUSTOMER'
      WHEN '30-Day Check-In' THEN 'CUSTOMER'
      WHEN 'Customer LO Sentiment' THEN 'CUSTOMER'
      WHEN 'Verified Connection - Customer' THEN 'CUSTOMER'
      WHEN '3WayIntro' THEN 'CUSTOMER'
      WHEN 'Inactive Outreach' THEN 'CUSTOMER'
      {% if target.type == 'snowflake' %}
      ELSE cpad.data:agent.role::VARCHAR
      END AS profile_role,
    CASE trim(cpad.data:campaign::VARCHAR)
      {% else %}
      ELSE cpad.data->'agent'->>'role'
      END AS profile_role,
    CASE trim(cpad.data->>'campaign')
      {% endif %}
      WHEN 'Agent LO Sentiment' THEN 'Verified to Pending'
      WHEN '45-Day Check-In' THEN 'Verified to Pending'
      WHEN 'Verified Connection - Agent' THEN 'Enroll to Verify'
      WHEN 'On Hold Outreach' THEN 'Verified to Pending'
      WHEN '30-Day Check-In' THEN 'Verified to Pending'
      WHEN 'Customer LO Sentiment' THEN 'Verified to Pending'
      WHEN 'Verified Connection - Customer' THEN 'Enroll to Verify'
      WHEN '3WayIntro' THEN null
      WHEN 'Inactive Outreach' THEN 'Verified to Pending'
      ELSE null
      END AS customer_journey,
    {% if target.type == 'snowflake' %}
    cpad.data:agent.phoneNumber::VARCHAR as profile_phone,
    cpad.data:agent.leadId::VARCHAR as lead_id,
    cpad.data:agent.aggregate_id::VARCHAR as aggregate_id,
    cpad.customer_id as customer_aggregate_id,
    cpad.data:questionResponse::VARCHAR as questionResponse,
    TRIM(replace(cpad.data:response::VARCHAR,'\n','')) as response,
    CONVERT_TIMEZONE('UTC', cpad.timestamp::VARCHAR) as system_date,
    CONVERT_TIMEZONE('America/Chicago', cpad.timestamp::VARCHAR) as reporting_date,
    cpad.customer_identifiers:email::VARCHAR as email,
    cpad.customer_identifiers:cio_id::VARCHAR as cio_id,
    {% else %}
    cpad.data->'agent'->>'phoneNumber' as profile_phone,
    cpad.data->'agent'->>'leadId' as lead_id,
    cpad.data->'agent'->>'aggregate_id' as aggregate_id,
    cpad.customer_id as customer_aggregate_id,
    cpad.data->>'questionResponse' as questionResponse,
    TRIM(replace(cpad.data->>'response',E'\n','')) as response,
    to_timestamp(cpad.timestamp) at time zone 'UTC' as system_date,
    to_timestamp(cpad.timestamp) at time zone 'CST' as reporting_date,
    cpad.customer_identifiers->>'email' as email,
    cpad.customer_identifiers->>'cio_id' as cio_id,
    {% endif %}
    'text' as delivery_type,
    cpad.type  as status
  from {{ source('public', 'cio_prod_activities') }} cpad
  where cpad.name ='outreach'
)
, lead_role_customer_cte as (
  select 
    distinct lead_id, role,profile_aggregate_id::text as profile_aggregate_id
  from {{ ref("current_assignments") }}
  where profile_aggregate_id in (select  customer_aggregate_id::{{ uuid_formatter() }} from source_cte where aggregate_id is null )
)
, int_cte as (
	select
    source_cte.campaign_name,
    source_cte.campaign_id,
    source_cte.AgentObj,
    source_cte.profile_role,
    source_cte.profile_phone,
    coalesce(source_cte.lead_id,lead_role_customer_cte.lead_id::text)::bigint as lead_id,
    source_cte.aggregate_id as agent_aggregate_id,
    source_cte.customer_aggregate_id,
    source_cte.questionResponse,
    source_cte.response,
    source_cte.system_date,
    source_cte.reporting_date,
    source_cte.email,
    source_cte.cio_id,
    source_cte.delivery_type,
    source_cte.status,
    source_cte.customer_journey
  from source_cte
	left join lead_role_customer_cte
	on source_cte.customer_aggregate_id = lead_role_customer_cte.profile_aggregate_id
	and source_cte.aggregate_id is null
)
, final_cte as (
	select
	source_cte.campaign_name,
	source_cte.campaign_id,
  source_cte.AgentObj,
  source_cte.profile_role,
  source_cte.profile_phone,
  source_cte.lead_id,
	coalesce(source_cte.agent_aggregate_id,ca.profile_aggregate_id::text) as agent_aggregate_id,
	ca.profile_aggregate_id::text as aggregate_id,
  source_cte.customer_aggregate_id,
  source_cte.questionResponse,
  source_cte.response,
  source_cte.system_date,
  source_cte.reporting_date,
	source_cte.email,
	source_cte.cio_id,
	source_cte.delivery_type,
  source_cte.status,
  source_cte.customer_journey
	from int_cte source_cte
	left join {{ ref('current_assignments') }} ca 
	on ca.lead_id = source_cte.lead_id
	and ca.{{  special_column_name_formatter('role') }} = source_cte.profile_role
)
select * from final_cte