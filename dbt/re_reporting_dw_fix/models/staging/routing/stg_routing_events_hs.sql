{% if target.type == 'snowflake' %}
with cte AS (
select
    cast(_airbyte_data:leadId::VARCHAR as bigint) as lead_id,
    _airbyte_data:leadRankParams[0].profileIds as agent_ids,
    _airbyte_data:leadRankParams as leadRankParams,
    _airbyte_data:smName::VARCHAR as smName,
   _airbyte_data:eventType::VARCHAR as event_type,
    cast(case when _airbyte_data:agentAggregateId::VARCHAR = 'empty' then null else _airbyte_data:agentAggregateId::VARCHAR end as {{ uuid_formatter() }}) as action_agent,
    _airbyte_data:routingMethod::VARCHAR as routing_method,
    _airbyte_data:tokenID::VARCHAR as invite_token,
    _airbyte_data:timestamp::VARCHAR as timestamp,
    _airbyte_data:routingStartTime::VARCHAR as routing_start_time,
    _airbyte_data:routingInitiatedTime::VARCHAR as routing_initiated_time,
    ARRAY_SIZE(_airbyte_data:agents) as agent_count,
    cast(_airbyte_data:agents[0].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent1,
    cast(_airbyte_data:agents[1].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent2,
    cast(_airbyte_data:agents[2].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent3,
    cast(_airbyte_data:agents[3].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent4,
    cast(_airbyte_data:agents[4].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent5,
    cast(_airbyte_data:agents[5].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent6,
    cast(_airbyte_data:agents[6].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent7,
    cast(_airbyte_data:agents[7].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent8,
    cast(_airbyte_data:agents[8].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent9,
    cast(_airbyte_data:agents[9].aggregateId::VARCHAR as {{ uuid_formatter() }}) as agent10
    from {{ source('public', '_AIRBYTE_RAW_PROD_ROUTING_API_STACK_SMEVENTSTABLE334CCEC7_NC661IJ28M3R') }}
)
,
cte_2 AS (
    select
    cast(_airbyte_data:leadId::VARCHAR as bigint) as lead_id,
    lrp.value:limit::VARCHAR as agent_limit,
    lrp.value:parentProfile::VARCHAR as parent_profile_id,
    lrp.value:parentDivisionName::VARCHAR as parent_division,
    ARRAY_SIZE(_airbyte_data:leadRankParams) as hla_selected_agent_count
    from {{ source('public', '_AIRBYTE_RAW_PROD_ROUTING_API_STACK_SMEVENTSTABLE334CCEC7_NC661IJ28M3R') }} rs,
    lateral flatten(input => rs._airbyte_data:leadRankParams) lrp
)
select
    Distinct
    a.*,
    b.agent_limit,
    b.parent_profile_id,
    b.parent_division,
    b.hla_selected_agent_count,
    COALESCE(a.agent_ids[0]::VARCHAR,NULL) as hla_selected_agent1,
    COALESCE(a.agent_ids[1]::VARCHAR,NULL) as hla_selected_agent2,
    COALESCE(a.agent_ids[2]::VARCHAR,NULL) as hla_selected_agent3
from cte a
left join cte_2 b on b.lead_id = a.lead_id
{% else %}
with cte AS (
select 
    cast(_airbyte_data->>'leadId' as bigint) as lead_id,
    _airbyte_data->'leadRankParams'->0->'profileIds' as agent_ids,
    _airbyte_data->'leadRankParams' as leadRankParams,
    _airbyte_data->>'smName' as smName,
   _airbyte_data->>'eventType' as event_type,
    cast(case when _airbyte_data->>'agentAggregateId' = 'empty' then null else _airbyte_data->>'agentAggregateId' end as uuid) as action_agent,
    _airbyte_data->>'routingMethod' as routing_method,
    _airbyte_data->>'tokenID' as invite_token,
    _airbyte_data->>'timestamp' as timestamp,
    _airbyte_data->>'routingStartTime' as routing_start_time,
    _airbyte_data->>'routingInitiatedTime' as routing_initiated_time,
    jsonb_array_length(_airbyte_data->'agents') as agent_count,
    cast(_airbyte_data->'agents'->0->>'aggregateId' as uuid) as agent1,
    cast(_airbyte_data->'agents'->1->>'aggregateId' as uuid) as agent2,
    cast(_airbyte_data->'agents'->2->>'aggregateId' as uuid) as agent3,
    cast(_airbyte_data->'agents'->3->>'aggregateId' as uuid) as agent4,
    cast(_airbyte_data->'agents'->4->>'aggregateId' as uuid) as agent5,
    cast(_airbyte_data->'agents'->5->>'aggregateId' as uuid) as agent6,
    cast(_airbyte_data->'agents'->6->>'aggregateId' as uuid) as agent7,
    cast(_airbyte_data->'agents'->7->>'aggregateId' as uuid) as agent8,
    cast(_airbyte_data->'agents'->8->>'aggregateId' as uuid) as agent9,
    cast(_airbyte_data->'agents'->9->>'aggregateId' as uuid) as agent10
    from {{ source('public', '_airbyte_raw_prod_routing_api_stack_smeventstable334ccec7_nc661') }}
)
,
cte_2 AS (
    select 
    cast(_airbyte_data->>'leadId' as bigint) as lead_id,
    jsonb_array_elements(_airbyte_data->'leadRankParams')->>'limit' as agent_limit,
    jsonb_array_elements(_airbyte_data->'leadRankParams')->>'parentProfile' as parent_profile_id,
    jsonb_array_elements(_airbyte_data->'leadRankParams')->>'parentDivisionName' as parent_division,
    jsonb_array_length(_airbyte_data->'leadRankParams') as hla_selected_agent_count
    from {{ source('public', '_airbyte_raw_prod_routing_api_stack_smeventstable334ccec7_nc661') }}
)
select 
    Distinct
    a.*,
    b.agent_limit,
    b.parent_profile_id,
    b.parent_division,
    b.hla_selected_agent_count,
    COALESCE(a.agent_ids->>0,NULL) as hla_selected_agent1,
    COALESCE(a.agent_ids->>1,NULL) as hla_selected_agent2,
    COALESCE(a.agent_ids->>2,NULL) as hla_selected_agent3
from cte a
left join cte_2 b on b.lead_id = a.lead_id
{% endif %}