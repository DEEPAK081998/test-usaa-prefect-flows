With lo_cte AS (
    select ca.lead_id,
    ca.profile_aggregate_id as mlo_aggregate_id,
    CONCAT(pup.first_name , ' ' , pup.last_name) AS inviting_lo,
    {% if target.type == 'snowflake' %}
    pup.data:jobTitle::VARCHAR as job_title,
    {% else %}
    pup.data->>'jobTitle' as job_title,
    {% endif %}
    pup.first_name as inviting_lo_firstname,
    pup.last_name as inviting_lo_lastname,
    {% if target.type == 'snowflake' %}
    pup.phones[0].phoneNumber::VARCHAR as inviting_lo_phone,
    pup.email as inviting_lo_email,
    pup.data:nmlsid::VARCHAR as mlo_nmlsid,
    {% else %}
    pup.phones->0->>'phoneNumber' as inviting_lo_phone,
    pup.email as inviting_lo_email, 
    pup.data->>'nmlsid' as mlo_nmlsid,
    {% endif %}
    row_number() over (partition by ca.lead_id order by ca.created asc) as row_id
    from {{ ref('current_assignments') }} ca 
    left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
    where ca.role = 'MLO'
)
,
inviting_lo_cte AS (
SELECT 
    lsu.lead_id as lead_id,
    loc.inviting_lo_email,
    loc.inviting_lo,
    loc.inviting_lo_firstname,
    loc.inviting_lo_lastname,
    loc.job_title,
    loc.inviting_lo_phone,
    loc.mlo_aggregate_id as inviting_aggregate_id,
    loc.mlo_nmlsid as inviting_nmlsid
  
FROM 
    {{ ref('lead_status_updates') }} lsu
    left join lo_cte loc on loc.lead_id = lsu.lead_id
where lsu.status like '%Invited Unaccepted%' and row_id = 1
order by lead_id ASC
)
,
mlo_hierarchy_cte AS (
{% if target.type == 'snowflake' %}
    select
'Citizens' as bank_name,
pup.data:nmlsid::VARCHAR as mlo_nmlsid,
concat(pup.first_name,' ',pup.last_name) as mlo_name,
pup.data:jobTitle::VARCHAR as enroll_job_title,
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
pup.data:dsm.name as dsm_division,
pup.data:dsm.email as dsm_email,
pup.aggregate_id,
'true' as enabled,
pure.role,
pup.updated,
ROW_NUMBER() over (PARTITION BY
pup.data:nmlsid::VARCHAR order by pup.updated DESC) as row_id
 from {{ ref('partner_user_profiles') }} pup
 left join {{ ref('partner_user_roles') }} pure on pure.user_profile_id = pup.id
 left outer join (
            select aggregate_id,
            min(PhoneNumber) as mlo_office_phone
            from (select aggregate_id,
                    fp.value:phoneType::VARCHAR as PhoneType,
                    fp.value:phoneNumber::VARCHAR as phoneNumber
                    from {{ ref('partner_user_profiles') }} pup,
                    lateral flatten(input => pup.phones) fp) pop
                    where lower(pop.Phonetype) = 'office'
                    group by aggregate_id) mop on mop.aggregate_id = pup.aggregate_id
        left outer join (
            select aggregate_id,
            min(PhoneNumber) as mlo_mobile_phone
            from (select aggregate_id,
                fp.value:phoneType::VARCHAR as PhoneType,
                fp.value:phoneNumber::VARCHAR as phoneNumber
                from {{ ref('partner_user_profiles') }} pup,
                lateral flatten(input => pup.phones) fp) pop
                where lower(pop.Phonetype) = 'mobile'
                group by aggregate_id) mmp on mmp.aggregate_id = pup.aggregate_id
where email like '%citizens%' and pure.role = 'MLO'
{% else %}
    select 
'Citizens' as bank_name,
pup.data->>'nmlsid' as mlo_nmlsid,
concat(pup.first_name,' ',pup.last_name) as mlo_name,
pup.data->>'jobTitle' as enroll_job_title,
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
pup.data->'dsm'->'name' as dsm_division,
pup.data->'dsm'->'email' as dsm_email,
pup.aggregate_id,
'true' as enabled,
pure.role,
pup.updated,
ROW_NUMBER() over (PARTITION BY 
pup.data->>'nmlsid' order by pup.updated DESC) as row_id     
 from {{ ref('partner_user_profiles') }} pup
 left join {{ ref('partner_user_roles') }} pure on pure.user_profile_id = pup.id 
 left outer join (
            select aggregate_id, 
            min(PhoneNumber) as mlo_office_phone
            from (select aggregate_id, 
                    json_array_elements(phones)->>'phoneType' as PhoneType,
                    json_array_elements(phones)->>'phoneNumber' as phoneNumber
                    from {{ ref('partner_user_profiles') }}) pop
                    where lower(pop.Phonetype) = 'office'
                    group by aggregate_id) mop on mop.aggregate_id = pup.aggregate_id
        left outer join (
            select aggregate_id, 
            min(PhoneNumber) as mlo_mobile_phone
            from (select aggregate_id, 
                json_array_elements(phones)->>'phoneType' as PhoneType,
                json_array_elements(phones)->>'phoneNumber' as phoneNumber
                from {{ ref('partner_user_profiles') }}) pop
                where lower(pop.Phonetype) = 'mobile'
                group by aggregate_id) mmp on mmp.aggregate_id = pup.aggregate_id  
where email like '%citizens%' and pure.role = 'MLO'
{% endif %}
)
,
mlo_final as (
    select * 
    from mlo_hierarchy_cte
    where row_id = 1
)
select ilc.*,
ld.*,
hc.apsm_manager_name as inviting_apsm_manager_name,
hc.apsm_email as inviting_apsm_email,
hc.psm_manager_name as inviting_psm_manager_name,
hc.psm_email as inviting_psm_email,
hc.asm_region as inviting_asm_region,
hc.asm_manager_name as inviting_asm_manager_name,
hc.asm_email as inviting_asm_email,
hc.rsm_region as inviting_rsm_region,
hc.rsm_manager_name as inviting_rsm_manager_name,
hc.rsm_email as inviting_rsm_email,
hc.dsm_manager_name as inviting_dsm_manager_name,
hc.dsm_email as inviting_dsm_email,
hc.dsm_division as inviting_dsm_division,
mhc.enroll_job_title,
mhc.apsm_manager_name as enrollment_apsm_manager_name,
mhc.apsm_email as enrollment_apsm_email,
mhc.psm_manager_name as enrollment_psm_manager_name,
mhc.psm_email as enrollment_psm_email,
mhc.asm_region as enrollment_asm_region,
mhc.asm_manager_name as enrollment_asm_manager_name,
mhc.asm_email as enrollment_asm_email,
mhc.rsm_region as enrollment_rsm_region,
mhc.rsm_manager_name as enrollment_rsm_manager_name,
mhc.rsm_email as enrollment_rsm_email,
mhc.dsm_manager_name as enrollment_dsm_manager_name,
mhc.dsm_email as enrollment_dsm_email,
mhc.dsm_division as enrollment_dsm_division
from inviting_lo_cte ilc 
full join {{ ref('leads_data_v3') }} ld on ld.id = ilc.lead_id 
left join mlo_final mhc on mhc.mlo_nmlsid = ld.nmlsid
left join mlo_final hc on hc.mlo_nmlsid = ilc.inviting_nmlsid
where ld.bank_name = 'Citizens' 
order by ilc.lead_id asc
