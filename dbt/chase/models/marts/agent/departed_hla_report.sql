with cte AS (
select 
    ap.*,
    nullif(ap.hlanmlsid,'')::int as nmlsid_join,
    agentscore,
    closescore,
    updatescore,
    responsescore
 from {{ ref('agent_profile_info') }} ap 
left join {{ ref('cae_agent_rc_scorecard') }} cars on cars.aggregate_id = ap.aggregate_id
)
select * from cte