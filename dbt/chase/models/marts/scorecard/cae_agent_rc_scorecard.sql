{{
  config(
    materialized = 'table',
    enabled = true
    )
}}
with cte as (
	select child_profile_id as agent_aggregate_id,
	parent_profile_id as rc_aggregate_id,
	pup.first_name as rc_first_name,
	pup.last_name as rc_last_name,
	pup.email as rc_email,
    pup.updated
	from {{ ref('partner_user_relationships') }} pur 
	left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = pur.parent_profile_id
	left join {{ ref('partner_user_roles') }} p on p.user_profile_id = pup.id
	where p.role = 'REFERRAL_COORDINATOR' and pur.enabled = 'true'
)
,
scorecard_join as (
    select 
        *
    from {{ ref('fct_agent_scorecard') }}
)
,
scorecard as (
select 
    aggregate_id,
    agentacceptcount,
    case when agentacceptcount<=3 then 3
     when closePct >= .15 then 5
     when closePct >=.1 then 4
     when closePct >=.09 then 3
     when closePct >=.08 then 2
     when closePct <.08 then 1
else 3 end as CloseScore,
    closepct,
    case when agentOffercount > 0 then (updates_in_compliance::decimal/agentOfferCount::decimal) end as updatePCT,
    responsepct,
    case when agentoffercount = 0 then 3
    when responsepct>=.80 then 5
    when responsepct>=.75 then 4
    when responsepct>=.5 then 3
    when responsepct>=.3 then 2
    when responsepct <.3 then 1
    else 3 end as ResponseScore,
    currentreferrals,
    currentreferrals_over90days
from scorecard_join
)
,
scorecard_final as (
select 
    s.*,
    case when agentacceptcount=0 then 3
     when updatePCT >=.9 then 5     
     when updatePCT >=.80 then 4
     when updatePCT >=.75 then 3
     when updatePCT >=.5 then 2
     when updatePCT <.5 then 1
    else 3 end as UpdateScore
from scorecard s
)
,agent_score as (
select 
    sf.*,
    (coalesce(CloseScore,3)+ coalesce(ResponseScore,3)+coalesce(UpdateScore,3))/3 as agentScore
from scorecard_final sf
)
,
final as (
select 
    distinct
	ap.first_name as agent_first_name,
    ap.last_name as agent_last_name,
    ap.aggregate_id,
    ap.brokeragecode,
    ap.brokerage_name,
	cte.rc_first_name,
	cte.rc_last_name,
	cte.rc_email,
    current_date as snapshot_date,
    sc.agentscore,
    round(sc.closepct,2) as closepct,
    sc.closescore,
    round(sc.updatepct,2) as updatepct,
    sc.updatescore,
    round(sc.responsepct,2) as responsepct,
    sc.responsescore,
    sc.currentreferrals,
    cte.updated
from {{ ref('agent_profile_info') }} ap
left join cte on cte.agent_aggregate_id = ap.aggregate_id
left join agent_score sc on sc.aggregate_id = ap.aggregate_id
where ap.verificationstatus ilike '%verified%' or ap.verificationstatus ilike '%reverification%'
)
,
dedup_rc as (
select 
    *,
    ROW_NUMBER() OVER (partition by aggregate_id ORDER BY updated DESC) AS row_number
from final 
)
select * from dedup_rc 
where row_number = 1