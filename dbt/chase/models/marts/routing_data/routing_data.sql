WITH routing_data AS(
    select t1.id as lead_id, t1.created lead_created, lsu.id as route_id,
    case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'zip' else nl.normalized_purchase_location->>'zip' end as normalizedZip,
    case when t1.transaction_type = 'SELL' then concat('Z',nl.normalized_sell_location->>'zip') else concat('Z',nl.normalized_purchase_location->>'zip') end as stringZip,
    case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'city' else nl.normalized_purchase_location->>'city' end as normalizedCity,
    case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'state' else nl.normalized_purchase_location->>'state' end as normalizedState,
    json_array_length(data->'profileIds') as routingPoolCount,
    data->>'profileIds' as routingPool,
    json_array_elements_text(data->'profileIds') as routing_agent_aggregate_id
    from {{ ref('leads') }}  t1 
    join {{ ref('lead_status_updates') }} lsu on t1.id = lsu.lead_id
    join {{ ref('normalized_lead_locations') }} nl on t1.id = nl.lead_id
    where status like '%Ready to Route%'
)
SELECT 
    *
FROM 
    routing_data
    LEFT JOIN {{ ref('agent_profiles') }} 
    ON routing_data.routing_agent_aggregate_id=CAST(agent_profiles.aggregate_id AS TEXT)