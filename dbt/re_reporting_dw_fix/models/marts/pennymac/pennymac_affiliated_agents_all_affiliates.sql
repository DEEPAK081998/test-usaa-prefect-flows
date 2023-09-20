with agent_affiliation_cte AS (
select pur.child_profile_uuid,
pur.parent_profile_uuid,
concat(pup.first_name,' ',pup.last_name) as agent_name,
pup.email as agent_email,
pup.brokerage_code,
pup.aggregate_id as agent_aggregate_id,
pup.verification_status,
{% if target.type == 'snowflake' %}
case when pup.brokerage_code is null then pup.data:brokerage.fullName::VARCHAR else b.full_name end as brokerage_name,
{% else %}
case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else b.full_name end as brokerage_name,
{% endif %}
concat(pop.first_name,' ',pop.last_name) as LO_name,
pop.aggregate_id as lo_aggregate_id,
case when pop.partner_id = 'e2a46d0a-6544-4116-8631-f08d749045ac' then replace(replace(pop.email,'pennymac.com','pnmac'),'.com','') else pop.email end as lo_email,
{% if target.type == 'snowflake' %}
pop.data:nmlsid::VARCHAR as lo_nmlsid,
{% else %}
pop.data->>'nmlsid' as lo_nmlsid,
{% endif %}
row_number() over
    (partition by pur.child_profile_uuid 
    order by pur.updated desc) as agent_row_id
 from {{ ref('partner_user_relationships') }} pur
 left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = pur.child_profile_uuid
 left join {{ ref('partner_user_roles') }} pure on pure.user_profile_id = pur.child_profile_id
 left join {{ ref('partner_user_profiles') }} pop on pop.aggregate_id = pur.parent_profile_uuid
 left join {{ ref('partner_user_roles') }} puree on puree.user_profile_id = pur.parent_profile_id
 left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
 where pure.role = 'AGENT' and puree.role = 'MLO' and (pop.email like '%pennymac%' OR pop.email like '%pnmac%')
    and concat(pop.first_name,' ',pop.last_name) <> lower(concat(pop.first_name,' ',pop.last_name))
)
,
agent_stats_cte AS (
    select pup.aggregate_id,
    count(distinct ca.lead_id) as total_referrals 
    from {{ ref('partner_user_profiles') }} pup 
    left join {{ ref('current_assignments') }} ca on ca.profile_aggregate_id = pup.aggregate_id
    group by pup.aggregate_id
)
,
agent_pennymac_stats_cte AS (
    select pup.aggregate_id,
    count(distinct ca.lead_id) as total_pennymac_referrals 
    from {{ ref('partner_user_profiles') }} pup 
    left join {{ ref('current_assignments') }} ca on ca.profile_aggregate_id = pup.aggregate_id 
    left join {{ ref('leads') }} t1 on t1.id = ca.lead_id
    where t1.bank_id = 'E2A46D0A-6544-4116-8631-F08D749045AC'
    group by pup.aggregate_id
)
,
pennymac_hierarchy_cte as (
    select 
        *
    from {{ ref('all_partner_hierarchy') }}
    where bank_name = 'PennyMac'
)
,
mlo_final as (
    select * 
    from pennymac_hierarchy_cte
   
),
closed_leads AS (
    select t1.agent_aggregate_id,
    count(distinct t1.id) as closed_leads
    from {{ ref('leads_data_v3') }} t1
    where t1.major_status = 'Closed' and t1.bank_name = 'PennyMac'
    group by t1.agent_aggregate_id
)
select aac.*,
chc.level_2_manager,
chc.level_2_manager_email,
chc.level_4_manager_name,
chc.level_4_manager_email,
chc.level_5_manager_name,
chc.level_5_manager_email,
chc.level_5_manager_region,
ascc.total_referrals,
acsc.total_pennymac_referrals,
cl.closed_leads as pennymac_closed_leads
from agent_affiliation_cte aac 
left join agent_stats_cte ascc on ascc.aggregate_id = aac.child_profile_uuid
left join agent_pennymac_stats_cte acsc on acsc.aggregate_id = aac.child_profile_uuid
left join mlo_final chc on chc.mlo_email_join = aac.lo_email
left join closed_leads cl on cl.agent_aggregate_id = aac.child_profile_uuid