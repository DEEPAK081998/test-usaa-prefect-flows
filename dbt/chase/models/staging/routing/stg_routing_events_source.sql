
 with cte AS (
 select 
    stateenteredeventdetails,
    stateenteredeventdetails::json->>'input' as input_field
from {{ source('public', 'routing_state_machine_v3_executions_history') }}
    where stateenteredeventdetails is not null
 
 ),
 v2 as (
 select 
    stateenteredeventdetails::json->>'name' as event,
    input_field::json->>'leads' as leads,/*->'leads'->>'id' as lead_id,*/
    input_field::json->>'source' as source,
    input_field::json->'leadIds'->>0 as lead_id_v2
 from cte
 )
 ,
 final AS (
 select 
    event,
    cast(coalesce(leads::json->0->>'id',lead_id_v2) as bigint) as lead_id,
    source 
 from v2
    where source is not null
 )

 select 
 distinct
 lead_id,
 source
 from final
where source = 'AUTO'
  
 