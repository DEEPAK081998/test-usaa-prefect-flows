
{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns',
        tags=['monthly']
    )
}}

{%- set table_exists = test_table_exists(this) -%}

{%- set rockscore_valid_condition = 'active_condition = 1' -%}

with activity_agent_bank_cte as (
    --summarizing activites all time, last 30 days and last 90 days for analysis
    select 
        pup.aggregate_id as agent_aggregate_id,
        program_name,
        aa.bank_id,
        count(distinct case when lower(agentactivity) ='accepted' then lead_id end) agentbank_acceptcount,
        count(distinct case when lower(agentactivity) ='reject' then lead_id end) agentbank_rejectcount,
        count(distinct case when lower(agentactivity) ='offered' then lead_id end) agentbank_offercount,
        count(distinct case when lower(agentactivity) ='ctc' then lead_id end) agentbank_ctccount,
        count(distinct case when lower(agentactivity) ='timeout' then lead_id end) agentbank_timeoutcount,
        count(distinct case when lower(agentactivity) ='update' then lead_id end) agentbank_statuscount,
        {% if target.type == 'snowflake' %}
        count(distinct case when lower(agentactivity) ='accepted' and datediff('day', aa.enrolleddate, current_date())<=30 then lead_id end) last30days_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) ='reject' and datediff('day', aa.enrolleddate, current_date())<=30 then lead_id end) last30days_agentbank_rejectcount,
        count(distinct case when lower(agentactivity) ='offered' and datediff('day', aa.enrolleddate, current_date())<=30 then lead_id end) last30days_agentbank_offercount,
        count(distinct case when lower(agentactivity) ='ctc' and datediff('day', aa.enrolleddate, current_date())<=30 then lead_id end) last30days_agentbank_ctccount,
        count(distinct case when lower(agentactivity) ='timeout' and datediff('day', aa.enrolleddate, current_date())<=30 then lead_id end) last30days_agentbank_timeoutcount,
        count(distinct case when lower(agentactivity) ='update' and datediff('day', aa.enrolleddate, current_date())<=30 then lead_id end) last30days_agentbank_statuscount,

        count(distinct case when lower(agentactivity) ='accepted' and datediff('day', aa.enrolleddate, current_date())<=90 then lead_id end) last90days_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) ='reject' and datediff('day', aa.enrolleddate, current_date())<=90 then lead_id end) last90days_agentbank_rejectcount,
        count(distinct case when lower(agentactivity) ='offered' and datediff('day', aa.enrolleddate, current_date())<=90 then lead_id end) last90days_agentbank_offercount,
        count(distinct case when lower(agentactivity) ='ctc' and datediff('day', aa.enrolleddate, current_date())<=90 then lead_id end) last90days_agentbank_ctccount,
        count(distinct case when lower(agentactivity) ='timeout' and datediff('day', aa.enrolleddate, current_date())<=90 then lead_id end) last90days_agentbank_timeoutcount,
        count(distinct case when lower(agentactivity) ='update' and datediff('day', aa.enrolleddate, current_date())<=90 then lead_id end) last90days_agentbank_statuscount,

        count(distinct case when lower(agentactivity) = 'accepted' and datediff('day', enrolleddate, current_date())::int between 90 and 456 then lead_id end) as l15mo_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) = 'accepted' and datediff('day', enrolleddate, current_date())::int <=365 then lead_id end) as l12mo_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) = 'reject' and datediff('day', enrolleddate, current_date())::int <=365 then lead_id end) as l12mo_agentbank_rejectcount,
        count(distinct case when lower(agentactivity) = 'offered' and datediff('day', enrolleddate, current_date())::int <=365 then lead_id end) as l12mo_agentbank_offeredcount

        {% else %}

        count(distinct case when lower(agentactivity) ='accepted' and extract(days from (current_date - aa.enrolleddate))::int<=30 then lead_id end) last30days_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) ='reject' and extract(days from (current_date - aa.enrolleddate))::int<=30 then lead_id end) last30days_agentbank_rejectcount,
        count(distinct case when lower(agentactivity) ='offered' and extract(days from (current_date - aa.enrolleddate))::int<=30 then lead_id end) last30days_agentbank_offercount,
        count(distinct case when lower(agentactivity) ='ctc' and extract(days from (current_date - aa.enrolleddate))::int<=30 then lead_id end) last30days_agentbank_ctccount,
        count(distinct case when lower(agentactivity) ='timeout' and extract(days from (current_date - aa.enrolleddate))::int<=30 then lead_id end) last30days_agentbank_timeoutcount,
        count(distinct case when lower(agentactivity) ='update' and extract(days from (current_date - aa.enrolleddate))::int<=30 then lead_id end) last30days_agentbank_statuscount,

        count(distinct case when lower(agentactivity) ='accepted' and extract(days from (current_date - aa.enrolleddate))::int<=90 then lead_id end) last90days_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) ='reject' and extract(days from (current_date - aa.enrolleddate))::int<=90 then lead_id end) last90days_agentbank_rejectcount,
        count(distinct case when lower(agentactivity) ='offered' and extract(days from (current_date - aa.enrolleddate))::int<=90 then lead_id end) last90days_agentbank_offercount,
        count(distinct case when lower(agentactivity) ='ctc' and extract(days from (current_date - aa.enrolleddate))::int<=90 then lead_id end) last90days_agentbank_ctccount,
        count(distinct case when lower(agentactivity) ='timeout' and extract(days from (current_date - aa.enrolleddate))::int<=90 then lead_id end) last90days_agentbank_timeoutcount,
        count(distinct case when lower(agentactivity) ='update' and extract(days from (current_date - aa.enrolleddate))::int<=90 then lead_id end) last90days_agentbank_statuscount,

        count(distinct case when lower(agentactivity) = 'accepted' and extract(days from (current_date - enrolleddate))::int between 90 and 456 then lead_id end) as l15mo_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) = 'accepted' and extract(days from (current_date - enrolleddate))::int <=365 then lead_id end) as l12mo_agentbank_acceptcount,
        count(distinct case when lower(agentactivity) = 'reject' and extract(days from (current_date - enrolleddate))::int <=365 then lead_id end) as l12mo_agentbank_rejectcount,
        count(distinct case when lower(agentactivity) = 'offered' and extract(days from (current_date - enrolleddate))::int <=365 then lead_id end) as l12mo_agentbank_offeredcount

        {% endif %}
    from 
        {{ ref('partner_user_profiles') }} pup
        join {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
        join {{ ref('fct_agent_referral_activities') }} aa on pup.aggregate_id = aa.agent_aggregate_id
    where 
        lower(pur.role) = 'agent'
    --and pur.enabled=true
    group by 
        pup.aggregate_id,
        program_name,
        aa.bank_id
),
activity_agent_cte as (
    select 
        fr.agent_aggregate_id,
        count(distinct case when lower(agentactivity) = 'accepted' then lead_id end) as agent_acceptcount,

        {% if target.type == 'snowflake' %}

        count(distinct case when lower(agentactivity) = 'accepted' and datediff('day', enrolleddate, current_date())::int between 90 and 456 then lead_id end) as l15mo_agent_acceptcount,
        count(distinct case when lower(agentactivity) = 'accepted' and datediff('day', enrolleddate, current_date())::int <=365 then lead_id end) as l12mo_agent_acceptcount,
        count(distinct case when lower(agentactivity) = 'reject' and datediff('day', enrolleddate, current_date())::int <=365 then lead_id end) as l12mo_agent_rejectcount,
        count(distinct case when lower(agentactivity) = 'offered' and datediff('day', enrolleddate, current_date())::int <=365 then lead_id end) as l12mo_agent_offeredcount

        {% else %}

        count(distinct case when lower(agentactivity) = 'accepted' and extract(days from (current_date - enrolleddate))::int between 90 and 456 then lead_id end) as l15mo_agent_acceptcount,
        count(distinct case when lower(agentactivity) = 'accepted' and extract(days from (current_date - enrolleddate))::int <=365 then lead_id end) as l12mo_agent_acceptcount,
        count(distinct case when lower(agentactivity) = 'reject' and extract(days from (current_date - enrolleddate))::int <=365 then lead_id end) as l12mo_agent_rejectcount,
        count(distinct case when lower(agentactivity) = 'offered' and extract(days from (current_date - enrolleddate))::int <=365 then lead_id end) as l12mo_agent_offeredcount

        {% endif %}
    from 
        fct_agent_referral_activities fr 
        join partner_user_profiles pup on fr.agent_aggregate_id = pup.aggregate_id
    group by 
        fr.agent_aggregate_id
),
activity_cte as (
	select

		ac_ag_bk.agent_aggregate_id,
        program_name,
        bank_id,
		
        agentbank_acceptcount,
        agentbank_rejectcount,
        agentbank_offercount,
        agentbank_ctccount,
        agentbank_timeoutcount,
        agentbank_statuscount,
        
        last30days_agentbank_acceptcount,
        last30days_agentbank_rejectcount,
        last30days_agentbank_offercount,
        last30days_agentbank_ctccount,
        last30days_agentbank_timeoutcount,
        last30days_agentbank_statuscount,

        last90days_agentbank_acceptcount,
        last90days_agentbank_rejectcount,
        last90days_agentbank_offercount,
        last90days_agentbank_ctccount,
        last90days_agentbank_timeoutcount,
        last90days_agentbank_statuscount,
		
        agent_acceptcount,
        l15mo_agent_acceptcount,
        l12mo_agent_acceptcount,
        l12mo_agent_rejectcount,
        l12mo_agent_offeredcount,

        l15mo_agentbank_acceptcount,
        l12mo_agentbank_acceptcount,
        l12mo_agentbank_rejectcount,
        l12mo_agentbank_offeredcount
        
	from
		activity_agent_bank_cte ac_ag_bk
		join activity_agent_cte ac_ag on ac_ag_bk.agent_aggregate_id = ac_ag.agent_aggregate_id
),
close_referrals_agent_cte as (
    select 
        agent_aggregate_id,

        {% if target.type == 'snowflake' %}

        count(distinct case when lower(major_status) = 'closed' and datediff('day', system_enroll_date, current_date())::int between 90 and 456 then id end) as l15mo_agent_closecount

        {% else %}

        count(distinct case when lower(major_status) = 'closed' and extract(days from (current_date - system_enroll_date))::int between 90 and 456 then id end) as l15mo_agent_closecount

        {% endif %}
    from 
        {{ ref('leads_data_v3') }} ld
    group by 
        agent_aggregate_id
),
close_referrals_agentbank_cte as (
    select 
        agent_aggregate_id,
        bank_id,

        {% if target.type == 'snowflake' %}

        count(distinct case when lower(major_status) = 'closed' and datediff('day', system_enroll_date, current_date())::int between 90 and 456 then id end) as l15mo_agentbank_closecount

        {% else %}

        count(distinct case when lower(major_status) = 'closed' and extract(days from (current_date - system_enroll_date))::int between 90 and 456 then id end) as l15mo_agentbank_closecount

        {% endif %}
    from 
        {{ ref('leads_data_v3') }} ld
    group by 
        agent_aggregate_id,
        bank_id
),
referral_agent_bank_cte as (
    --current referral counts for each agent and bank
    select 
        profile_aggregate_id as agent_aggregate_id,
        t1.bank_id as bank_id,
        t1.bank_name as bank_name,
        count(distinct t1.id) as agentbank_currentreferrals,

        {% if target.type == 'snowflake' %}

        count(distinct case when datediff('day', t1.system_enroll_date, current_date) >= 90 then t1.id end) as agentbank_currentreferrals_over90days,
        count(distinct case when datediff('day', t1.system_enroll_date, current_date) <= 30 then t1.id end) as agentbank_currentreferrals_acceptedlast30days,
        count(distinct case when datediff('day', t1.system_enroll_date, current_date) <= 90 then t1.id end) as agentbank_currentreferrals_acceptedlast90days,
        count(distinct case when lower(major_status) = 'active' then t1.id end) as agentbank_activereferrals,
        count(distinct case when lower(major_status) = 'closed' then t1.id end) as agentbank_closedreferrals,
        count(distinct case when lower(major_status) = 'closed' and datediff('day', t1.system_enroll_date, current_date) >= 90 then t1.id end) as agentbank_closedreferrals_over90days,
        count(distinct case when lower(major_status) = 'inactive' then t1.id end) as agentbank_inactivereferrals,
        count(distinct case when lower(major_status) = 'pending' then t1.id end) as agentbank_pendingreferrals,
        count(distinct case when lower(major_status) = 'on hold' then t1.id end) as agentbank_onholdreferrals

        {% else %}

        count(distinct case when extract(days from (current_date - t1.system_enroll_date))::int >= 90 then t1.id end) as agentbank_currentreferrals_over90days,
        count(distinct case when extract(days from (current_date - t1.system_enroll_date))::int <= 30 then t1.id end) as agentbank_currentreferrals_acceptedlast30days,
        count(distinct case when extract(days from (current_date - t1.system_enroll_date))::int <= 90 then t1.id end) as agentbank_currentreferrals_acceptedlast90days,
        count(distinct case when lower(major_status) = 'active' then t1.id end) as agentbank_activereferrals,
        count(distinct case when lower(major_status) = 'closed' then t1.id end) as agentbank_closedreferrals,
        count(distinct case when lower(major_status) ='closed' and extract(days from (current_date - t1.system_enroll_date))::int >= 90 then t1.id end) as agentbank_closedreferrals_over90days,
        count(distinct case when lower(major_status) = 'inactive' then t1.id end) as agentbank_inactivereferrals,
        count(distinct case when lower(major_status) = 'pending' then t1.id end) as agentbank_pendingreferrals,
        count(distinct case when lower(hb_status) like '%on hold%' then t1.id end) as agentbank_onholdreferrals

        {% endif %}

    from {{ ref('leads_data_v3') }} t1
    join {{ ref('current_assignments') }} ca on t1.id = ca.lead_id
    where consumer_confirmed = true
    group by profile_aggregate_id, bank_name, bank_id
),
referrals_cte as (
	select

		ab_ref.agent_aggregate_id,
        ab_ref.bank_name,
        ab_ref.bank_id,

        l15mo_agentbank_closecount,
        l15mo_agent_closecount,
		
        agentbank_currentreferrals,
        agentbank_currentreferrals_over90days,
        agentbank_currentreferrals_acceptedlast30days,
        agentbank_currentreferrals_acceptedlast90days,
        agentbank_activereferrals,
        agentbank_closedreferrals,
        agentbank_closedreferrals_over90days,
        agentbank_inactivereferrals,
        agentbank_pendingreferrals,
        agentbank_onholdreferrals
		
	from
        referral_agent_bank_cte ab_ref
        left join close_referrals_agent_cte cl_ag_ref 
            on ab_ref.agent_aggregate_id = cl_ag_ref.agent_aggregate_id
        left join close_referrals_agentbank_cte cl_agbk_ref 
            on ab_ref.agent_aggregate_id = cl_agbk_ref.agent_aggregate_id 
            and ab_ref.bank_id = cl_agbk_ref.bank_id
),
activities_and_referral_cte as (
	--merge data from activities and referrals
	select

		coalesce(ac.agent_aggregate_id,rc.agent_aggregate_id) as agent_aggregate_id,
        coalesce(ac.program_name,rc.bank_name) as program_name,
        coalesce(ac.bank_id,rc.bank_id) as bank_id,
		
        agentbank_acceptcount,
        agentbank_rejectcount,
        agentbank_offercount,
        agentbank_ctccount,
        agentbank_timeoutcount,
        agentbank_statuscount,
        
        last30days_agentbank_acceptcount,
        last30days_agentbank_rejectcount,
        last30days_agentbank_offercount,
        last30days_agentbank_ctccount,
        last30days_agentbank_timeoutcount,
        last30days_agentbank_statuscount,
        last90days_agentbank_acceptcount,
        last90days_agentbank_rejectcount,
        last90days_agentbank_offercount,
        last90days_agentbank_ctccount,
        last90days_agentbank_timeoutcount,
        last90days_agentbank_statuscount,

        agent_acceptcount,
        l15mo_agent_acceptcount,
        l12mo_agent_acceptcount,
        l12mo_agent_rejectcount,
        l12mo_agent_offeredcount,

        l15mo_agentbank_acceptcount,
        l12mo_agentbank_acceptcount,
        l12mo_agentbank_rejectcount,
        l12mo_agentbank_offeredcount,

        l15mo_agent_closecount,
        l15mo_agentbank_closecount, 

		agentbank_currentreferrals,
        agentbank_currentreferrals_over90days,
        agentbank_currentreferrals_acceptedlast30days,
        agentbank_currentreferrals_acceptedlast90days,
        agentbank_activereferrals,
        agentbank_closedreferrals,
        agentbank_closedreferrals_over90days,
        agentbank_inactivereferrals,
        agentbank_pendingreferrals,
        agentbank_onholdreferrals
		
	from
		activity_cte ac
		full outer join referrals_cte rc on ac.agent_aggregate_id = rc.agent_aggregate_id and ac.program_name = rc.bank_name
),
fct_agentbank_scorecard_cte as (
    select distinct
        pup.aggregate_id,
        pup.email as agent_email,
        pup.verification_status,
        arc.bank_id,
        arc.program_name,
        case when pms.agent_row_id = 1 then 1 else 0 end as panagent,

        case when coalesce(agentbank_offercount,0) > 0 then (coalesce(agentbank_acceptcount,0)::decimal/agentbank_offercount) else 0 end as agentbank_acceptpct,
        case when coalesce(agentbank_offercount,0) > 0 then (coalesce((agentbank_acceptcount+agentbank_rejectcount),0)::decimal/agentbank_offercount) else 0 end as agentbank_responsepct,
        case when coalesce(agentbank_offercount,0) > 0 then (coalesce(agentbank_timeoutcount,0)::decimal/agentbank_offercount) else 0 end as agentbank_timeoutpct,

        case when coalesce(last30days_agentbank_offercount,0) > 0 then (coalesce(last30days_agentbank_acceptcount,0)::decimal/last30days_agentbank_offercount) else 0 end as l30_agentbank_acceptpct,
        case when coalesce(last30days_agentbank_offercount,0) > 0 then (coalesce((last30days_agentbank_acceptcount+last30days_agentbank_rejectcount),0)::decimal/last30days_agentbank_offercount) else 0 end as l30_agentbank_responsepct,
        case when coalesce(last30days_agentbank_offercount,0) > 0 then (coalesce(last30days_agentbank_timeoutcount,0)::decimal/last30days_agentbank_offercount) else 0 end as l30_agentbank_timeoutpct,

        case when coalesce(last90days_agentbank_offercount,0) > 0 then (coalesce(last90days_agentbank_acceptcount,0)::decimal/last90days_agentbank_offercount) else 0 end as l90_agentbank_acceptpct,
        case when coalesce(last90days_agentbank_offercount,0) > 0 then (coalesce((last90days_agentbank_acceptcount+last90days_agentbank_rejectcount),0)::decimal/last90days_agentbank_offercount) else 0 end as l90_agentbank_responsepct,
        case when coalesce(last90days_agentbank_offercount,0) > 0 then (coalesce(last90days_agentbank_timeoutcount,0)::decimal/last90days_agentbank_offercount) else 0 end as l90_agentbank_timeoutpct,
        case when coalesce(agentbank_currentreferrals_over90days,0) > 0  then (coalesce(agentbank_closedreferrals_over90days,0)::decimal/agentbank_currentreferrals_over90days) else 0 end as o90_agentbank_closepct,
        case when coalesce(last90days_agentbank_offercount,0) > 0 then (coalesce(aum.last90days_total_referrals_in_compliance,0)::decimal/last90days_agentbank_offercount) else 0 end as l90_agentbank_updatepct,

        case when coalesce(l15mo_agent_acceptcount,0) > 0 then (coalesce(l15mo_agent_closecount,0)::decimal/l15mo_agent_acceptcount) else 0 end as l15mo_agent_closepct,
        case when coalesce(l12mo_agent_offeredcount,0) > 0 then (coalesce(l12mo_agent_acceptcount,0)::decimal/l12mo_agent_offeredcount) else 0 end as l12mo_agent_acceptpct,
        case when coalesce(l12mo_agent_offeredcount,0) > 0 then (coalesce((l12mo_agent_acceptcount+l12mo_agent_rejectcount),0)::decimal/l12mo_agent_offeredcount) else 0 end as l12mo_agent_responsepct,
        case when coalesce(l15mo_agentbank_acceptcount,0) > 0 then (coalesce(l15mo_agentbank_closecount,0)::decimal/l15mo_agentbank_acceptcount) else 0 end as l15mo_agentbank_closepct,
        case when coalesce(l12mo_agentbank_offeredcount,0) > 0 then (coalesce(l12mo_agentbank_acceptcount,0)::decimal/l12mo_agentbank_offeredcount) else 0 end as l12mo_agentbank_acceptpct,
        case when coalesce(l12mo_agentbank_offeredcount,0) > 0 then (coalesce((l12mo_agentbank_acceptcount+l12mo_agentbank_rejectcount),0)::decimal/l12mo_agentbank_offeredcount) else 0 end as l12mo_agentbank_responsepct,

        coalesce(agentbank_acceptcount,0) as agentbank_acceptcount, 
        coalesce(agentbank_rejectcount,0) as agentbank_rejectcount,
        coalesce(agentbank_offercount,0) as agentbank_offercount,
        coalesce(agentbank_ctccount,0) as agentbank_ctccount,
        coalesce(agentbank_timeoutcount,0) as agentbank_timeoutcount,
        coalesce(agentbank_statuscount,0) as agentbank_statuscount,
        coalesce(last30days_agentbank_acceptcount,0) as last30days_agentbank_acceptcount,
        coalesce(last30days_agentbank_rejectcount,0) as last30days_agentbank_rejectcount,
        coalesce(last30days_agentbank_offercount,0) as last30days_agentbank_offercount,
        coalesce(last30days_agentbank_ctccount,0) as last30days_agentbank_ctccount,
        coalesce(last30days_agentbank_timeoutcount,0) as last30days_agentbank_timeoutcount,
        coalesce(last30days_agentbank_statuscount,0) as last30days_agentbank_statuscount,
        coalesce(last90days_agentbank_acceptcount,0) as last90days_agentbank_acceptcount,
        coalesce(last90days_agentbank_rejectcount,0) as last90days_agentbank_rejectcount,
        coalesce(last90days_agentbank_offercount,0) as last90days_agentbank_offercount,
        coalesce(last90days_agentbank_ctccount,0) as last90days_agentbank_ctccount,
        coalesce(last90days_agentbank_timeoutcount,0) as last90days_agentbank_timeoutcount,
        coalesce(last90days_agentbank_statuscount,0) as last90days_agentbank_statuscount,
        coalesce(agentbank_currentreferrals,0) as agentbank_currentreferrals,
        coalesce(agentbank_currentreferrals_over90days,0) as agentbank_currentreferrals_over90days,
        coalesce(agentbank_currentreferrals_acceptedlast30days,0) as agentbank_currentreferrals_acceptedlast30days,
        coalesce(agentbank_currentreferrals_acceptedlast90days,0) as agentbank_currentreferrals_acceptedlast90days,
        coalesce(agentbank_activereferrals,0) as agentbank_activereferrals,
        coalesce(agentbank_closedreferrals,0) as agentbank_closedreferrals,
        coalesce(agentbank_closedreferrals_over90days,0) as agentbank_closedreferrals_over90days,
        coalesce(agentbank_inactivereferrals,0) as agentbank_inactivereferrals,
        coalesce(agentbank_pendingreferrals,0) as agentbank_pendingreferrals,
        coalesce(agentbank_onholdreferrals,0) as agentbank_onholdreferrals,
        coalesce(aum.total_referrals_in_compliance,0) as agentbank_updates_in_compliance,
        coalesce(aum.last30days_total_referrals_in_compliance,0) last30days_agentbank_updates_in_compliance,
        coalesce(aum.last90days_total_referrals_in_compliance,0) last90days_agentbank_updates_in_compliance,
        coalesce(agent_acceptcount,0) as agent_acceptcount,
        coalesce(l15mo_agent_acceptcount,0) as l15mo_agent_acceptcount,
        coalesce(l12mo_agent_acceptcount,0) as l12mo_agent_acceptcount,
        coalesce(l12mo_agent_rejectcount,0) as l12mo_agent_rejectcount,
        coalesce(l12mo_agent_offeredcount,0) as l12mo_agent_offeredcount,
        coalesce(l15mo_agent_closecount,0) as l15mo_agent_closecount,
        coalesce(l15mo_agentbank_acceptcount,0) as l15mo_agentbank_acceptcount,
        coalesce(l12mo_agentbank_acceptcount,0) as l12mo_agentbank_acceptcount,
        coalesce(l12mo_agentbank_rejectcount,0) as l12mo_agentbank_rejectcount,
        coalesce(l12mo_agentbank_offeredcount,0) as l12mo_agentbank_offeredcount,
        coalesce(l15mo_agentbank_closecount,0) as l15mo_agentbank_closecount
        
    from {{ ref('partner_user_profiles') }} pup
        left join activities_and_referral_cte arc on pup.aggregate_id = arc.agent_aggregate_id
        left join {{ ref('fct_agent_update_activities') }} aum on pup.aggregate_id = aum.agent_aggregate_id and arc.bank_id =  aum.bank_id
        left join pennymac_affiliated_agents_w_stats pms on pup.aggregate_id = pms.agent_aggregate_id
    where pup.id in (select user_profile_id from {{ ref("partner_user_roles") }} where lower(role)='agent')
),
scores_cte as
(
	select 
		aggregate_id,
		bank_id,
		o90_agentbank_closepct, 
		l90_agentbank_responsepct, 
		l90_agentbank_updatepct,
		case when agentbank_acceptcount<3 then 3
			when o90_agentbank_closepct >= .15 then 5
			when o90_agentbank_closepct >=.1 then 4
			when o90_agentbank_closepct >=.09 then 3
			when o90_agentbank_closepct >=.08 then 2
			when o90_agentbank_closepct <.08 then 1
			else 3 end as agentbank_closescore,
		case when last90days_agentbank_offercount = 0 then 3
			when l90_agentbank_responsepct>=.80 then 5
			when l90_agentbank_responsepct>=.75 then 4
			when l90_agentbank_responsepct>=.5 then 3
			when l90_agentbank_responsepct>=.3 then 2
			when l90_agentbank_responsepct >=0 then 1
			else 3 end as agentbank_responsescore,
		case when last90days_agentbank_acceptcount=0 then 3
			when l90_agentbank_updatepct >=.9 then 5     
			when l90_agentbank_updatepct >=.80 then 4
			when l90_agentbank_updatepct >=.75 then 3
			when l90_agentbank_updatepct >=.5 then 2
			when l90_agentbank_updatepct >=0 then 1
			else 3 end as agentbank_updatescore
	from 
		fct_agentbank_scorecard_cte
	),
stg_fct_agentbank_scorecard as (
    select
        current_timestamp as score_created_at,
        fct.aggregate_id,
        fct.agent_email,
        fct.bank_id,
        fct.program_name,
        fct.verification_status,
        fct.panagent,
        fct.agentbank_acceptpct,
        fct.agentbank_responsepct,
        fct.agentbank_timeoutpct,
        fct.l30_agentbank_acceptpct,
        fct.l30_agentbank_responsepct,
        fct.l30_agentbank_timeoutpct,
        fct.l90_agentbank_acceptpct,
        fct.l90_agentbank_responsepct,
        fct.l90_agentbank_timeoutpct,
        fct.o90_agentbank_closepct,
        fct.l90_agentbank_updatepct,
        l15mo_agent_closepct,
        l12mo_agent_acceptpct,
        l12mo_agent_responsepct,
        fct.agentbank_acceptcount,
        fct.agentbank_rejectcount,
        fct.agentbank_offercount,
        fct.agentbank_ctccount,
        fct.agentbank_timeoutcount,
        fct.agentbank_statuscount,
        fct.last30days_agentbank_acceptcount,
        fct.last30days_agentbank_rejectcount,
        fct.last30days_agentbank_offercount,
        fct.last30days_agentbank_ctccount,
        fct.last30days_agentbank_timeoutcount,
        fct.last30days_agentbank_statuscount,
        fct.last90days_agentbank_acceptcount,
        fct.last90days_agentbank_rejectcount,
        fct.last90days_agentbank_offercount,
        fct.last90days_agentbank_ctccount,
        fct.last90days_agentbank_timeoutcount,
        fct.last90days_agentbank_statuscount,
        fct.agentbank_currentreferrals,
        fct.agentbank_currentreferrals_over90days,
        fct.agentbank_currentreferrals_acceptedlast30days,
        fct.agentbank_currentreferrals_acceptedlast90days,
        fct.agentbank_activereferrals,
        fct.agentbank_closedreferrals,
        fct.agentbank_closedreferrals_over90days,
        fct.agentbank_inactivereferrals,
        fct.agentbank_pendingreferrals,
        fct.agentbank_onholdreferrals,
        fct.agentbank_updates_in_compliance,
        fct.last30days_agentbank_updates_in_compliance,
        fct.last90days_agentbank_updates_in_compliance,
        agent_acceptcount,
        l15mo_agent_acceptcount,
        l12mo_agent_acceptcount,
        l12mo_agent_rejectcount,
        l12mo_agent_offeredcount,
        l15mo_agent_closecount,
        l15mo_agentbank_acceptcount,
        l12mo_agentbank_acceptcount,
        l12mo_agentbank_rejectcount,
        l12mo_agentbank_offeredcount,
        l15mo_agentbank_closecount,                  
        coalesce(scr.agentbank_closescore,3) as agentbank_closescore, 
        coalesce(scr.agentbank_responsescore,3) as agentbank_responsescore, 
        coalesce(scr.agentbank_updatescore,3) as agentbank_updatescore, 
        (coalesce(scr.agentbank_closescore,3)+ coalesce(scr.agentbank_responsescore,3)+coalesce(scr.agentbank_updatescore,3))::decimal/3 as agentbank_score,
        avg((coalesce(scr.agentbank_closescore,3)+ coalesce(scr.agentbank_responsescore,3)+coalesce(scr.agentbank_updatescore,3))::decimal/3)
            over (partition by fct.aggregate_id) as agent_overallascore,
        {{ dynamic_case_when(ref('rockscore'),'agent_rockscore','condition_result','condition',rockscore_valid_condition) }},
        {{ dynamic_case_when(ref('rockscore'),'agentbank_rockscore','condition_result','condition',rockscore_valid_condition) }}
    from fct_agentbank_scorecard_cte fct
        join scores_cte scr on fct.aggregate_id = scr.aggregate_id and fct.bank_id = scr.bank_id
)

{% if table_exists %}
,
score_version_id_cte as (
    select
        aggregate_id,
        bank_id,
        max(score_version_id) as score_version_id
    from
        {{ this }}
    group by
        aggregate_id,
        bank_id
)

{% endif %}

select
	score_created_at,

    {% if table_exists %}

    (coalesce(scr.score_version_id,0) + 1) as score_version_id,

    {% else %}

    1 as score_version_id,

    {% endif %}

	stg.aggregate_id,
    agent_email,
	stg.bank_id,
	program_name,
	verification_status,
    panagent,
	agentbank_acceptpct,
	agentbank_responsepct,
	agentbank_timeoutpct,
	l30_agentbank_acceptpct,
	l30_agentbank_responsepct,
	l30_agentbank_timeoutpct,
	l90_agentbank_acceptpct,
	l90_agentbank_responsepct,
	l90_agentbank_timeoutpct,
	o90_agentbank_closepct,
	l90_agentbank_updatepct,
    l15mo_agent_closepct,
    l12mo_agent_acceptpct,
    l12mo_agent_responsepct,
	agentbank_acceptcount,
	agentbank_rejectcount,
	agentbank_offercount,
	agentbank_ctccount,
	agentbank_timeoutcount,
	agentbank_statuscount,
	last30days_agentbank_acceptcount,
	last30days_agentbank_rejectcount,
	last30days_agentbank_offercount,
	last30days_agentbank_ctccount,
	last30days_agentbank_timeoutcount,
	last30days_agentbank_statuscount,
	last90days_agentbank_acceptcount,
	last90days_agentbank_rejectcount,
	last90days_agentbank_offercount,
	last90days_agentbank_ctccount,
	last90days_agentbank_timeoutcount,
	last90days_agentbank_statuscount,
	agentbank_currentreferrals,
	agentbank_currentreferrals_over90days,
	agentbank_currentreferrals_acceptedlast30days,
	agentbank_currentreferrals_acceptedlast90days,
	agentbank_activereferrals,
	agentbank_closedreferrals,
	agentbank_closedreferrals_over90days,
	agentbank_inactivereferrals,
	agentbank_pendingreferrals,
	agentbank_onholdreferrals,
	agentbank_updates_in_compliance,
	last30days_agentbank_updates_in_compliance,
	last90days_agentbank_updates_in_compliance,
    agent_acceptcount,
    l15mo_agent_acceptcount,
    l12mo_agent_acceptcount,
    l12mo_agent_rejectcount,
    l12mo_agent_offeredcount,
    l15mo_agent_closecount,
    l15mo_agentbank_acceptcount,
    l12mo_agentbank_acceptcount,
    l12mo_agentbank_rejectcount,
    l12mo_agentbank_offeredcount,
    l15mo_agentbank_closecount,             
	agentbank_closescore,
	agentbank_responsescore,
	agentbank_updatescore,
	agentbank_score,
	agent_overallascore,
    agentbank_rockscore,
    agent_rockscore
from 
    stg_fct_agentbank_scorecard stg

    {% if table_exists %}

    left join score_version_id_cte scr on stg.aggregate_id = scr.aggregate_id and stg.bank_id = scr.bank_id

    {% endif %}