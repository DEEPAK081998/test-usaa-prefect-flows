{{
  config(
    materialized = 'table'
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
        program_name,
        aa.bank_id,
        COUNT(CASE WHEN agentactivity ='Accepted' THEN 1 END) agentAcceptcount,
        COUNT(CASE WHEN agentactivity ='Reject' THEN 1 END) agentRejectCount,
        COUNT(CASE WHEN agentactivity ='Offered' THEN 1 END) agentOfferCount,
        COUNT(CASE WHEN agentactivity ='CTC' THEN 1 END) agentCTCCount,
        COUNT(CASE WHEN agentactivity ='Timeout' THEN 1 END) agentTimeoutCount,
        COUNT(CASE WHEN agentactivity ='Update' THEN 1 END) agentStatusCount,
        {% if target.type == 'snowflake' %}
        COUNT(CASE WHEN agentactivity ='Accepted' and DATEDIFF('day', aa.created, CURRENT_DATE())<=30 THEN 1 END) Last30Days_agentAcceptcount,
        COUNT(CASE WHEN agentactivity ='Reject' and DATEDIFF('day', aa.created, CURRENT_DATE())<=30 THEN 1 END) Last30Days_agentRejectCount,
        COUNT(CASE WHEN agentactivity ='Offered' and DATEDIFF('day', aa.created, CURRENT_DATE())<=30 THEN 1 END) Last30Days_agentOfferCount,
        COUNT(CASE WHEN agentactivity ='CTC' and DATEDIFF('day', aa.created, CURRENT_DATE())<=30 THEN 1 END) Last30Days_agentCTCCount,
        COUNT(CASE WHEN agentactivity ='Timeout' and DATEDIFF('day', aa.created, CURRENT_DATE())<=30 THEN 1 END) Last30Days_agentTimeoutCount,
        COUNT(CASE WHEN agentactivity ='Update' and DATEDIFF('day', aa.created, CURRENT_DATE())<=30 THEN 1 END) Last30Days_agentStatusCount,

        COUNT(CASE WHEN agentactivity ='Accepted' and DATEDIFF('day', aa.created, CURRENT_DATE())<=90 THEN 1 END) Last90Days_agentAcceptcount,
        COUNT(CASE WHEN agentactivity ='Reject' and DATEDIFF('day', aa.created, CURRENT_DATE())<=90 THEN 1 END) Last90Days_agentRejectCount,
        COUNT(CASE WHEN agentactivity ='Offered' and DATEDIFF('day', aa.created, CURRENT_DATE())<=90 THEN 1 END) Last90Days_agentOfferCount,
        COUNT(CASE WHEN agentactivity ='CTC' and DATEDIFF('day', aa.created, CURRENT_DATE())<=90 THEN 1 END) Last90Days_agentCTCCount,
        COUNT(CASE WHEN agentactivity ='Timeout' and DATEDIFF('day', aa.created, CURRENT_DATE())<=90 THEN 1 END) Last90Days_agentTimeoutCount,
        COUNT(CASE WHEN agentactivity ='Update' and DATEDIFF('day', aa.created, CURRENT_DATE())<=90 THEN 1 END) last90days_agentstatuscount
        {% else %}
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
        {% endif %}
    FROM {{ ref('partner_user_profiles') }} pup
    JOIN {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
    JOIN {{ ref('fct_agent_referral_activities') }} aa on pup.aggregate_id = aa.agent_aggregate_id
    WHERE pur.role = 'AGENT'
    --and pur.enabled=true
    GROUP BY pup.aggregate_id,program_name,aa.bank_id
),
referral_counts as (
    --current referral counts for each agent
    select 
        profile_aggregate_id as agent_aggregate_id,
        t1.bank_id as bank_id,
        t1.bank_name as bank_name,
        coalesce(count(distinct t1.id),0) as currentReferrals,
        {% if target.type == 'snowflake' %}
        COALESCE(COUNT(DISTINCT CASE WHEN DATEDIFF('day', t1.system_enroll_date, CURRENT_DATE) >= 90 THEN t1.id END), 0) AS currentReferrals_Over90days,
        COALESCE(COUNT(DISTINCT CASE WHEN DATEDIFF('day', ca.created, CURRENT_DATE) <= 30 THEN t1.id END), 0) AS currentReferrals_AcceptedLast30days,
        COALESCE(COUNT(DISTINCT CASE WHEN DATEDIFF('day', ca.created, CURRENT_DATE) <= 90 THEN t1.id END), 0) AS currentReferrals_AcceptedLast90days,
        COALESCE(COUNT(DISTINCT CASE WHEN major_status = 'Active' THEN t1.id END), 0) AS activeReferrals,
        COALESCE(COUNT(DISTINCT CASE WHEN major_status = 'Closed' THEN t1.id END), 0) AS closedReferrals,
        COALESCE(COUNT(DISTINCT CASE WHEN major_status = 'Closed' AND DATEDIFF('day', t1.system_enroll_date, CURRENT_DATE) >= 90 THEN t1.id END), 0) AS closedReferrals_over90Days,
        COALESCE(COUNT(DISTINCT CASE WHEN major_status = 'Inactive' THEN t1.id END), 0) AS inactiveReferrals,
        COALESCE(COUNT(DISTINCT CASE WHEN major_status = 'Pending' THEN t1.id END), 0) AS pendingReferrals,
        COALESCE(COUNT(DISTINCT CASE WHEN major_status = 'On Hold' THEN t1.id END), 0) AS onHoldReferrals
        {% else %}
        coalesce(count(distinct t1.id) FILTER (WHERE extract(days from (current_date - t1.system_enroll_date))::int>=90),0) as currentReferrals_Over90days,
        coalesce(count(distinct t1.id) FILTER (WHERE extract(days from (current_date - ca.created))::int<=30),0) as currentReferrals_AcceptedLast30days,
        coalesce(count(distinct t1.id) FILTER (WHERE extract(days from (current_date - ca.created))::int<=90),0) as currentReferrals_AcceptedLast90days,
        coalesce(count(distinct t1.id) FILTER (WHERE major_status = 'Active'),0) as activeReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE major_status = 'Closed'),0) as closedReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE major_status ='Closed' and extract(days from (current_date - t1.system_enroll_date))::int>=90),0) as closedReferrals_over90Days,
        coalesce(count(distinct t1.id) FILTER (WHERE major_status = 'Inactive'),0) as inactiveReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE major_status = 'Pending'),0) as pendingReferrals,
        coalesce(count(distinct t1.id) FILTER (WHERE major_status = 'On Hold'),0) as onHoldReferrals
        {% endif %}

    from {{ ref('leads_data_v3') }} t1
    join {{ ref('current_assignments') }} ca on t1.id = ca.lead_id
    where consumer_confirmed = true
    group by profile_aggregate_id, bank_name, bank_id
),
activities_and_referral_counts as (
	--merge data from activities and referral counts
	select

		coalesce(ac.agent_aggregate_id,rc.agent_aggregate_id) as agent_aggregate_id,
        coalesce(ac.program_name,rc.bank_name) as program_name,
        coalesce(ac.bank_id,rc.bank_id) as bank_id,
		
        agentacceptcount,
        agentrejectcount,
        agentoffercount,
        agentctccount,
        agenttimeoutcount,
        agentstatuscount,
        
        last30days_agentacceptcount,
        last30days_agentrejectcount,
        last30days_agentoffercount,
        last30days_agentctccount,
        last30days_agenttimeoutcount,
        last30days_agentstatuscount,

        last90days_agentacceptcount,
        last90days_agentrejectcount,
        last90days_agentoffercount,
        last90days_agentctccount,
        last90days_agenttimeoutcount,
        last90days_agentstatuscount,
		
        currentreferrals,
        
        currentreferrals_over90days,
        currentreferrals_acceptedlast30days,
        currentreferrals_acceptedlast90days,
        activereferrals,
        closedreferrals,
        closedreferrals_over90days,
        inactivereferrals,
        pendingreferrals,
        onholdreferrals
		
	from
		activity_counts ac
		full outer join referral_counts rc on ac.agent_aggregate_id = rc.agent_aggregate_id and ac.program_name = rc.bank_name
),
fct_agent_scorecard_cte as (
    select
        pup.aggregate_id,
        pup.verification_status,
        arc.bank_id,
        arc.program_name,

        case when coalesce(agentoffercount,0) > 0 then (agentacceptcount::decimal/agentoffercount) else 0 end as acceptpct,
        case when agentoffercount > 0 then ((agentacceptcount+agentrejectcount)::decimal/agentoffercount) else 0 end as responsepct,
        case when agentoffercount > 0 then (agenttimeoutcount::decimal/agentoffercount) else 0 end as timeoutpct,
        case when last30days_agentoffercount > 0 then (last30days_agentacceptcount::decimal/last30days_agentoffercount) else 0 end as l30_acceptpct,
        case when last30days_agentoffercount > 0 then ((last30days_agentacceptcount+last30days_agentrejectcount)::decimal/last30days_agentoffercount) else 0 end as l30_responsepct,
        case when last30days_agentoffercount > 0 then (last30days_agenttimeoutcount::decimal/last30days_agentoffercount) else 0 end as l30_timeoutpct,
        case when last90days_agentoffercount > 0 then (last90days_agentacceptcount::decimal/last90days_agentoffercount) else 0 end as l90_acceptpct,
        case when last90days_agentoffercount > 0 then ((last90days_agentacceptcount+last90days_agentrejectcount)::decimal/last90days_agentoffercount) else 0 end as l90_responsepct,
        case when last90days_agentoffercount > 0 then (last90days_agenttimeoutcount::decimal/last90days_agentoffercount) else 0 end as l90_timeoutpct,
        case when currentreferrals_over90days > 0  then (closedreferrals_over90days::decimal/currentreferrals_over90days) else 0 end as closepct,
        case when Last90Days_agentOffercount > 0 then (coalesce(aum.last90days_total_referrals_in_compliance,0)::decimal/Last90Days_agentOfferCount) else 0 end as L90_updatePCT,
        coalesce(agentacceptcount,0) as agentacceptcount, 
        coalesce(agentrejectcount,0) as agentrejectcount,
        coalesce(agentoffercount,0) as agentoffercount,
        coalesce(agentctccount,0) as agentctccount,
        coalesce(agenttimeoutcount,0) as agenttimeoutcount,
        coalesce(agentstatuscount,0) as agentstatuscount,
        coalesce(last30days_agentacceptcount,0) as last30days_agentacceptcount,
        coalesce(last30days_agentrejectcount,0) as last30days_agentrejectcount,
        coalesce(last30days_agentoffercount,0) as last30days_agentoffercount,
        coalesce(last30days_agentctccount,0) as last30days_agentctccount,
        coalesce(last30days_agenttimeoutcount,0) as last30days_agenttimeoutcount,
        coalesce(last30days_agentstatuscount,0) as last30days_agentstatuscount,
        coalesce(last90days_agentacceptcount,0) as last90days_agentacceptcount,
        coalesce(last90days_agentrejectcount,0) as last90days_agentrejectcount,
        coalesce(last90days_agentoffercount,0) as last90days_agentoffercount,
        coalesce(last90days_agentctccount,0) as last90days_agentctccount,
        coalesce(last90days_agenttimeoutcount,0) as last90days_agenttimeoutcount,
        coalesce(last90days_agentstatuscount,0) as last90days_agentstatuscount,
        coalesce(currentreferrals,0) as currentreferrals,
        coalesce(currentreferrals_over90days,0) as currentreferrals_over90days,
        coalesce(currentreferrals_acceptedlast30days,0) as currentreferrals_acceptedlast30days,
        coalesce(currentreferrals_acceptedlast90days,0) as currentreferrals_acceptedlast90days,
        coalesce(activereferrals,0) as activereferrals,
        coalesce(closedreferrals,0) as closedreferrals,
        coalesce(closedreferrals_over90days,0) as closedreferrals_over90days,
        coalesce(inactivereferrals,0) as inactivereferrals,
        coalesce(pendingreferrals,0) as pendingreferrals,
        coalesce(onholdreferrals,0) as onholdreferrals,
        coalesce(aum.total_referrals_in_compliance,0) as updates_in_compliance,
        coalesce(aum.last30days_total_referrals_in_compliance,0) last30days_updates_in_compliance,
        coalesce(aum.last90days_total_referrals_in_compliance,0) last90days_updates_in_compliance
        
    from {{ ref('partner_user_profiles') }} pup
    --join {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
    --join {{ ref('stg_pup_banks') }} stg on  pup.partner_id = stg.partner_id
        left join activities_and_referral_counts arc on pup.aggregate_id = arc.agent_aggregate_id
        left join {{ ref('fct_agent_update_activities') }} aum on pup.aggregate_id = aum.agent_aggregate_id and arc.bank_id =  aum.bank_id
    where pup.id in (select user_profile_id from {{ ref("partner_user_roles") }} where lower(role)='agent')
),
scores_cte as
(
	select 
		aggregate_id,
		bank_id,
		closePct, 
		l90_responsepct, 
		L90_updatePCT,
		case when agentacceptcount<3 then 3
			when closePct >= .15 then 5
			when closePct >=.1 then 4
			when closePct >=.09 then 3
			when closePct >=.08 then 2
			when closePct <.08 then 1
			else 3 end as CloseScore,
		case when Last90Days_agentOffercount = 0 then 3
			when l90_responsepct>=.80 then 5
			when l90_responsepct>=.75 then 4
			when l90_responsepct>=.5 then 3
			when l90_responsepct>=.3 then 2
			when l90_responsepct >=0 then 1
			else 3 end as ResponseScore,
		case when last90days_agentacceptcount=0 then 3
			when L90_updatePCT >=.9 then 5     
			when L90_updatePCT >=.80 then 4
			when L90_updatePCT >=.75 then 3
			when L90_updatePCT >=.5 then 2
			when L90_updatePCT >=0 then 1
			else 3 end as UpdateScore
	from 
		fct_agent_scorecard_cte
	)
select
	fct.aggregate_id,
	fct.verification_status,
	fct.bank_id,
	fct.program_name,
	fct.acceptpct,
	fct.responsepct,
	fct.timeoutpct,
	fct.l30_acceptpct,
	fct.l30_responsepct,
	fct.l30_timeoutpct,
	fct.l90_acceptpct,
	fct.l90_responsepct,
	fct.l90_timeoutpct,
	fct.closepct,
	fct.l90_updatepct,
	fct.agentacceptcount,
	fct.agentrejectcount,
	fct.agentoffercount,
	fct.agentctccount,
	fct.agenttimeoutcount,
	fct.agentstatuscount,
	fct.last30days_agentacceptcount,
	fct.last30days_agentrejectcount,
	fct.last30days_agentoffercount,
	fct.last30days_agentctccount,
	fct.last30days_agenttimeoutcount,
	fct.last30days_agentstatuscount,
	fct.last90days_agentacceptcount,
	fct.last90days_agentrejectcount,
	fct.last90days_agentoffercount,
	fct.last90days_agentctccount,
	fct.last90days_agenttimeoutcount,
	fct.last90days_agentstatuscount,
	fct.currentreferrals,
	fct.currentreferrals_over90days,
	fct.currentreferrals_acceptedlast30days,
	fct.currentreferrals_acceptedlast90days,
	fct.activereferrals,
	fct.closedreferrals,
	fct.closedreferrals_over90days,
	fct.inactivereferrals,
	fct.pendingreferrals,
	fct.onholdreferrals,
	fct.updates_in_compliance,
	fct.last30days_updates_in_compliance,
	fct.last90days_updates_in_compliance,
	coalesce(scr.CloseScore,3) as CloseScore, 
	coalesce(scr.ResponseScore,3) as ResponseScore, 
	coalesce(scr.UpdateScore,3) as UpdateScore, 
	(coalesce(scr.CloseScore,3)+ coalesce(scr.ResponseScore,3)+coalesce(scr.UpdateScore,3))/3 as agentScore,
	avg((coalesce(scr.CloseScore,3)+ coalesce(scr.ResponseScore,3)+coalesce(scr.UpdateScore,3))/3)
		over (partition by fct.aggregate_id) as overallAgentScore
 from fct_agent_scorecard_cte fct
	join scores_cte scr on fct.aggregate_id = scr.aggregate_id and fct.bank_id = scr.bank_id