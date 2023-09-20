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
    from {{ source('public', 'raw_routing_events_chase') }}
)
,
cte_2 AS (
    select 
    cast(_airbyte_data->>'leadId' as bigint) as lead_id,
    jsonb_array_elements(_airbyte_data->'leadRankParams')->>'limit' as agent_limit,
    jsonb_array_elements(_airbyte_data->'leadRankParams')->>'parentProfile' as parent_profile_id,
    jsonb_array_elements(_airbyte_data->'leadRankParams')->>'parentDivisionName' as parent_division,
    jsonb_array_length(_airbyte_data->'leadRankParams') as hla_selected_agent_count
    from {{ source('public', 'raw_routing_events_chase') }}
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
    