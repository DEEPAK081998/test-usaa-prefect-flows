with cte AS (
     select 
     stateenteredeventdetails::json as json_field,
     stateenteredeventdetails::json->>'input' as input_field
     from {{ source('public', 'legacy_step_functions_executions_history') }}
 )
 ,
 request_data_cte AS (
select *,
json_field::json->>'name' as nameState,
input_field::json->>'requestData' as request_data,
input_field::json->'requestData'->>'routingToken' as routing_token,
--(input_field::json->'requestData'->'payload'->'routings'->0->'leadIds')->>0 as lead_id,
input_field::json->'lambdaResult'->>'state' as lambda_result,
input_field::json->'lambdaResult'->>'status' as lambda_status,
input_field::json->>'waitValue' as waitValue,
row_number() over() as rowid
from cte
 )
 /*,
 lambda_cte AS (
     select lambda_result::json as lambda_state
     from request_data_cte
 )*/
 ,
 payload_cte AS (
    select request_data::json->>'payload' as payload,
    request_data::json->>'routingToken' as routing_token
    from request_data_cte
 )
 ,
 routings_cte AS (
 select payload::json->>'routings' as routings
 from payload_cte
 )
 ,
 lead_cte AS (
     select routings::json->0->'leadIds'->>0 as lead_id,
     row_number() over() as row_num
     from routings_cte
 )
 ,
 final AS (
 select lc.lead_id,
rdc.* ,
row_number() over (partition by lc.lead_id order by rdc.waitvalue desc) as row_number_final
 from request_data_cte rdc
 left join lead_cte lc on lc.row_num = rdc.rowid
 where rdc.json_field is not null and rdc.lambda_status is not null
 )
 select * from final where row_number_final = 1