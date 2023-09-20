with agent_affiliation_cte AS (
select pur.child_profile_uuid,
pur.parent_profile_uuid,
concat(pup.first_name,' ',pup.last_name) as agent_name,
pup.email as agent_email,
pup.brokerage_code,
{% if target.type == 'snowflake' %}
case when pup.brokerage_code is null then pup.data:brokerage.fullName::VARCHAR else b.full_name end as brokerage_name,
concat(pop.first_name,' ',pop.last_name) as LO_name,
pop.email as lo_email,
pop.data:nmlsid::VARCHAR as lo_nmlsid
{% else %}
case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else b.full_name end as brokerage_name,
concat(pop.first_name,' ',pop.last_name) as LO_name,
pop.email as lo_email,
pop.data->>'nmlsid' as lo_nmlsid
{% endif %}
 from {{ ref('partner_user_relationships') }} pur
 left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = pur.child_profile_uuid
 left join {{ ref('partner_user_roles') }} pure on pure.user_profile_id = pur.child_profile_id
 left join {{ ref('partner_user_profiles') }} pop on pop.aggregate_id = pur.parent_profile_uuid
 left join {{ ref('partner_user_roles') }} puree on puree.user_profile_id = pur.parent_profile_id
 left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
 where pure.role = 'AGENT' and puree.role = 'MLO' 
)
,
agent_affiliation AS (
    select * 
    from agent_affiliation_cte
    where lo_email like '%citizens%'
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
agent_citizens_stats_cte AS (
    select pup.aggregate_id,
    count(distinct ca.lead_id) as total_citizens_referrals 
    from {{ ref('partner_user_profiles') }} pup 
    left join {{ ref('current_assignments') }} ca on ca.profile_aggregate_id = pup.aggregate_id 
    left join {{ ref('leads') }} t1 on t1.id = ca.lead_id
    where t1.bank_id = '06D7EEA6-A312-4233-B53B-DE52EA1C240E'
    group by pup.aggregate_id
)
,
citizens_hierarchy_cte as (
{% if target.type == 'snowflake' %}
    select
pup.updated,
'Citizens' as bank_name,
pup.data:nmlsid::VARCHAR as mlo_nmlsid,
concat(pup.first_name,' ',pup.last_name) as mlo_name,
pup.email as mlo_email,
coalesce(mop.mlo_office_phone,mmp.mlo_mobile_phone) as mlo_phone,
pup.data:apsm.managerName::VARCHAR as apsm_manager_name,
pup.data:apsm.email::VARCHAR as apsm_email,
pup.data:psm.managerName::VARCHAR as psm_manager_name,
pup.data:psm.email::VARCHAR as psm_email,
pup.data:asm.name::VARCHAR as asm_region,
pup.data:asm.managerName::VARCHAR as asm_manager_name,
pup.data:asm.email::VARCHAR as asm_email,
pup.data:rsm.name::VARCHAR as rsm_region,
pup.data:rsm.managerName::VARCHAR as rsm_manager_name,
pup.data:rsm.email::VARCHAR as rsm_email,
pup.data:dsm.managerName::VARCHAR as dsm_manager_name,
pup.data:dsm.name::VARCHAR as dsm_division,
pup.data:dsm.email::VARCHAR as dsm_email,
pup.aggregate_id,
'true' as enabled,
pure.role,
ROW_NUMBER() over (PARTITION BY
pup.data:nmlsid::VARCHAR order by pup.updated DESC) as row_id
{% else %}
    select 
pup.updated,
'Citizens' as bank_name,
pup.data->>'nmlsid' as mlo_nmlsid,
concat(pup.first_name,' ',pup.last_name) as mlo_name,
pup.email as mlo_email,
coalesce(mop.mlo_office_phone,mmp.mlo_mobile_phone) as mlo_phone,
pup.data->'apsm'->>'managerName' as apsm_manager_name,
pup.data->'apsm'->>'email' as apsm_email,
pup.data->'psm'->>'managerName' as psm_manager_name,
pup.data->'psm'->>'email' as psm_email,
pup.data->'asm'->>'name' as asm_region,
pup.data->'asm'->>'managerName' as asm_manager_name,
pup.data->'asm'->>'email' as asm_email,
pup.data->'rsm'->>'name' as rsm_region,
pup.data->'rsm'->>'managerName' as rsm_manager_name,
pup.data->'rsm'->>'email' as rsm_email,
pup.data->'dsm'->>'managerName' as dsm_manager_name,
pup.data->'dsm'->>'name' as dsm_division,
pup.data->'dsm'->>'email' as dsm_email,
pup.aggregate_id,
'true' as enabled,
pure.role,
ROW_NUMBER() over (PARTITION BY 
pup.data->>'nmlsid' order by pup.updated DESC) as row_id
{% endif %}
 from {{ ref('partner_user_profiles') }} pup
 left join {{ ref('partner_user_roles') }} pure on pure.user_profile_id = pup.id 
 left outer join (
            select aggregate_id, 
            min(PhoneNumber) as mlo_office_phone
            from (select aggregate_id,
                    {% if target.type == 'snowflake' %}
                    fp.value:phoneType::VARCHAR as PhoneType,
                    fp.value:phoneNumber::VARCHAR as phoneNumber
                    from {{ ref('partner_user_profiles') }} pup,
                    lateral flatten(input => pup.phones) fp
                    {% else %}
                    json_array_elements(phones)->>'phoneType' as PhoneType,
                    json_array_elements(phones)->>'phoneNumber' as phoneNumber
                    from {{ ref('partner_user_profiles') }}
                    {% endif %}
                    ) pop
                    where lower(pop.Phonetype) = 'office'
                    group by aggregate_id) mop on mop.aggregate_id = pup.aggregate_id
        left outer join (
            select aggregate_id, 
            min(PhoneNumber) as mlo_mobile_phone
            from (select aggregate_id,
                {% if target.type == 'snowflake' %}
                fp.value:phoneType::VARCHAR as PhoneType,
                fp.value:phoneNumber::VARCHAR as phoneNumber
                from {{ ref('partner_user_profiles') }} pup,
                lateral flatten(input => pup.phones) fp
                {% else %}
                json_array_elements(phones)->>'phoneType' as PhoneType,
                json_array_elements(phones)->>'phoneNumber' as phoneNumber
                from {{ ref('partner_user_profiles') }}
                {% endif %}
                ) pop
                where lower(pop.Phonetype) = 'mobile'
                group by aggregate_id) mmp on mmp.aggregate_id = pup.aggregate_id  
where email like '%citizens%' and pure.role = 'MLO'
)
,
mlo_final as (
    select * 
    from citizens_hierarchy_cte
    where row_id = 1
)
select aac.*,
chc.apsm_manager_name,
chc.apsm_email,
chc.psm_manager_name,
chc.psm_email,
chc.asm_region,
chc.asm_manager_name,
chc.asm_email,
chc.rsm_region,
chc.rsm_manager_name,
chc.rsm_email,
chc.dsm_division,
chc.dsm_manager_name,
chc.dsm_email,
ascc.aggregate_id,
ascc.total_referrals,
acsc.total_citizens_referrals
from agent_affiliation aac 
left join agent_stats_cte ascc on ascc.aggregate_id = aac.child_profile_uuid
left join agent_citizens_stats_cte acsc on acsc.aggregate_id = aac.child_profile_uuid
left join mlo_final chc on chc.mlo_nmlsid = aac.lo_nmlsid 