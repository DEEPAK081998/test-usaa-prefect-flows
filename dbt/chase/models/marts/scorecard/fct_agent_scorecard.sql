{{
  config(
    materialized = 'table',
    enabled = true
    )
}}
{#- 
    IF THERE IS AN UPDATE IN THIS MODEL PLEASE UPDATE THE scorecard_version ON macro snapshot_agent_scorecard
    CURRENT VERSION : v.1.0.0
 -#}
with activity_counts as (
    --summarizing activites all time, last 30 days and last 90 days for analysis
    select 
        pup.aggregate_id as agent_aggregate_id,
        'Chase' as program_name,
        --program_name,
        COUNT(CASE WHEN agentactivity ='Accepted' THEN 1 END) agentAcceptcount,
        COUNT(CASE WHEN agentactivity ='Reject' THEN 1 END) agentRejectCount,
        COUNT(CASE WHEN agentactivity ='Offered' THEN 1 END) agentOfferCount,
        COUNT(CASE WHEN agentactivity ='CTC' THEN 1 END) agentCTCCount,
        COUNT(CASE WHEN agentactivity ='Timeout' THEN 1 END) agentTimeoutCount,
        COUNT(CASE WHEN agentactivity ='Update' THEN 1 END) agentStatusCount,

        COUNT(CASE WHEN agentactivity ='Accepted' and extract(days from (current_date - aa.created))::int<=30 THEN 1 END) Last30Days_agentAcceptcount,
        COUNT(CASE WHEN agentactivity ='Reject' and extract(days from (current_date - aa.created))::int<=30 THEN 1 END) Last30Days_agentRejectCount,
        COUNT(CASE WHEN agentactivity ='Offered' and extract(days from (current_date - aa.created))::int<=30 THEN 1 END) Last30Days_agentOfferCount,
        COUNT(CASE WHEN agentactivity ='CTC' and extract(days from (current_date - aa.created))::int<=30 THEN 1 END) Last30Days_agentCTCCount,
        COUNT(CASE WHEN agentactivity ='Timeout' and extract(days from (current_date - aa.created))::int<=30 THEN 1 END) Last30Days_agentTimeoutCount,
        COUNT(CASE WHEN agentactivity ='Update' and extract(days from (current_date - aa.created))::int<=30 THEN 1 END) Last30Days_agentStatusCount,

        COUNT(CASE WHEN agentactivity ='Accepted' and extract(days from (current_date - aa.created))::int<=90 THEN 1 END) Last90Days_agentAcceptcount,
        COUNT(CASE WHEN agentactivity ='Reject' and extract(days from (current_date - aa.created))::int<=90 THEN 1 END) Last90Days_agentRejectCount,
        COUNT(CASE WHEN agentactivity ='Offered' and extract(days from (current_date - aa.created))::int<=90 THEN 1 END) Last90Days_agentOfferCount,
        COUNT(CASE WHEN agentactivity ='CTC' and extract(days from (current_date - aa.created))::int<=90 THEN 1 END) Last90Days_agentCTCCount,
        COUNT(CASE WHEN agentactivity ='Timeout' and extract(days from (current_date - aa.created))::int<=90 THEN 1 END) Last90Days_agentTimeoutCount,
        COUNT(CASE WHEN agentactivity ='Update' and extract(days from (current_date - aa.created))::int<=90 THEN 1 END) last90days_agentstatuscount
    FROM {{ ref('partner_user_profiles') }} pup
    JOIN {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
    LEFT JOIN {{ ref('fct_agent_referral_activities') }} aa on pup.aggregate_id = aa.agent_aggregate_id
    WHERE pur.role = 'AGENT' and pur.enabled=true
    GROUP BY pup.aggregate_id,program_name
),
referral_counts as (
    --current referral counts for each agent
    select 
        agentaggregateid as agent_aggregate_id,
        'Chase' as program_name,
        -- stg.bank_name as program_name,
        coalesce(count(distinct t1.id),0) as currentReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE extract(days from (current_date - enrolleddate))::int>=90),0) as currentReferrals_Over90days,
        coalesce(count(distinct t1.id) FILTER (WHERE extract(days from (current_date - ca.created))::int<=30),0) as currentReferrals_AcceptedLast30days,
        coalesce(count(distinct t1.id) FILTER (WHERE extract(days from (current_date - ca.created))::int<=90),0) as currentReferrals_AcceptedLast90days,
        coalesce(count(distinct t1.id) FILTER (WHERE majorstatus = 'Active'),0) as activeReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE majorstatus = 'Closed'),0) as closedReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE majorstatus ='Closed' and extract(days from (current_date - enrolleddate))::int>=90),0) as closedReferrals_over90Days,
        coalesce(count(distinct t1.id) FILTER (WHERE majorstatus = 'Inactive'),0) as inactiveReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE majorstatus = 'Pending'),0) as pendingReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE majorstatus = 'On Hold'),0) as onHoldReferrals

    from {{ ref('chase_enrollments_test') }} t1
    join {{ ref('current_assignments') }} ca on t1.id = ca.lead_id
    left join {{ ref('leads') }} t2 
      ON t1.id = t2.id
    left join {{ ref('stg_lead_banks') }} stg 
      on t2.bank_id = stg.bank_id
    where t1.consumer_confirmed = true
    group by agentaggregateid
)
SELECT
    pup.aggregate_id,
    case when pup.verification_status = 'Verified-Provisional' then 'Verified-Verified'
    when pup.verification_status = 'Verified-Select Only' then 'Verified-Verified'
    else pup.verification_status end as verification_status,
    pup.verification_status as internal_verificationstatus,
    'Chase' as program_name,
    --stg.bank_name as program_name,
    case when coalesce(agentOffercount,0) > 0 then agentAcceptcount/agentOfferCount end as acceptPct,
    case when agentOffercount > 0 then (agentAcceptcount+agentRejectCount)/agentOfferCount end as responsePct,
    case when agentOffercount > 0 then agentTimeoutCount/agentOfferCount end as timeoutPct,
    case when Last30Days_agentOffercount > 0 then (Last30Days_agentAcceptcount/Last30Days_agentOfferCount)::decimal end as L30_acceptPct,
    case when Last30Days_agentOffercount > 0 then ((Last30Days_agentAcceptcount+Last30Days_agentRejectCount)/Last30Days_agentOfferCount)::decimal end as L30_responsePct,
    case when Last30Days_agentOffercount > 0 then (Last30Days_agentTimeoutCount/Last30Days_agentOfferCount)::decimal end as L30_timeoutPct,
    case when Last90Days_agentOffercount > 0 then (Last90Days_agentAcceptcount/Last90Days_agentOfferCount)::decimal end as L90_acceptPct,
    case when Last90Days_agentOffercount > 0 then ((Last90Days_agentAcceptcount+Last90Days_agentRejectCount)/Last90Days_agentOfferCount)::decimal end as L90_responsePct,
    case when Last90Days_agentOffercount > 0 then (Last90Days_agentTimeoutCount/Last90Days_agentOfferCount)::decimal end as L90_timeoutPct,
    case when currentReferrals_Over90days > 0  then (closedReferrals_over90Days/currentReferrals_Over90days)::decimal end as closePCT,
    ac.agentAcceptcount, ac.agentRejectCount, ac.agentOfferCount,ac.agentCTCCount, ac.agentTimeoutCount, ac.agentstatuscount,
    ac.last30days_agentAcceptcount, ac.last30days_agentRejectCount, ac.last30days_agentOfferCount,ac.last30days_agentCTCCount, ac.last30days_agentTimeoutCount, ac.last30days_agentstatuscount,
    ac.last90days_agentAcceptcount, ac.last90days_agentRejectCount, ac.last90days_agentOfferCount,ac.last90days_agentCTCCount, ac.last90days_agentTimeoutCount, ac.last90days_agentstatuscount,
    rc.currentReferrals, rc.currentReferrals_Over90days, rc.currentReferrals_AcceptedLast30days, rc.currentReferrals_AcceptedLast90days,
    rc.activeReferrals, rc.closedReferrals, rc.closedReferrals_over90Days, rc.inactiveReferrals, rc.pendingReferrals, rc.onHoldReferrals,
    coalesce(aum.total_referrals_in_compliance,0) as updates_in_compliance,
    coalesce(aum.Last30Days_total_referrals_in_compliance,0) Last30days_updates_in_compliance,
    coalesce(aum.Last90days_total_referrals_in_compliance,0) Last90days_updates_in_compliance
from {{ ref('partner_user_profiles') }} pup
join {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
--join {{ ref('stg_pup_banks') }} stg on  pup.partner_id = stg.partner_id
left join activity_counts ac on pup.aggregate_id = ac.agent_aggregate_id -- and stg.bank_name = ac.program_name
left join referral_counts rc on pup.aggregate_id = rc.agent_aggregate_id -- and stg.bank_name = rc.program_name
left join {{ ref('fct_agent_update_activities') }} aum on pup.aggregate_id = aum.agent_aggregate_id
and pup.partner_id =  aum.bank_id
where lower(pur.role) = 'agent' and enabled = true