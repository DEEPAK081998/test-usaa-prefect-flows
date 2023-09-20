with hierarchy_cte AS (
select 
bank_name as bank_name,
lo_nmlsid as mlo_nmlsid,
lo_name as mlo_name,
case when bank_name  = 'PennyMac' then replace(lo_email,'.com','') else lo_email end as mlo_email_join,
lo_email as mlo_email,
lo_phone as mlo_phone,
'' as level_1_manager,
'' as level_1_manager_email,
lm_name as level_2_manager,
lm_email as level_2_manager_email, 
'' as level_3_manager_name,
'' as level_3_manager_email,
'' as level_3_manager_region,
slm_name as level_4_manager_name,
slm_manager as level_4_manager_email,
'' as level_4_manager_region,
dd_name as level_5_manager_name,
dd_email as level_5_manager_email,
division as level_5_manager_region,
1 as row_id
from {{ source('public', 'raw_partner_hierarchy') }}
UNION 
select 
'Citizens' as bank_name,
{% if target.type == 'snowflake' %}
pup.data:nmlsid::VARCHAR as mlo_nmlsid,
concat(pup.first_name,' ',pup.last_name) as mlo_name,
pup.email as mlo_email_join,
pup.email as mlo_email,
coalesce(mop.mlo_office_phone,mmp.mlo_mobile_phone) as mlo_phone,
pup.data:apsm.managerName::VARCHAR as level_1_manager,
pup.data:apsm.email::VARCHAR as level_1_manager_email,
pup.data:psm.managerName::VARCHAR as level_2_manager_name,
pup.data:psm:email::VARCHAR as level_2_manager_email,
pup.data:asm.managerName::VARCHAR as level_3_manager_name,
pup.data:asm.email::VARCHAR as level_3_manager_email,
pup.data:asm.name::VARCHAR as level_3_manager_region,
pup.data:rsm.managerName::VARCHAR as level_4_manager_name,
pup.data:rsm.email::VARCHAR as level_4_manager_email,
pup.data:rsm.name::VARCHAR as level_4_manager_region,
pup.data:dsm.managerName::VARCHAR as level_5_manager_name,
pup.data:dsm.email::VARCHAR as level_5_manager_email,
pup.data:dsm.name::VARCHAR as level_5_manager_region,
ROW_NUMBER() over (PARTITION BY
pup.data:nmlsid::VARCHAR order by pup.updated DESC) as row_id
{% else %}
pup.data->>'nmlsid' as mlo_nmlsid,
concat(pup.first_name,' ',pup.last_name) as mlo_name,
pup.email as mlo_email_join,
pup.email as mlo_email,
coalesce(mop.mlo_office_phone,mmp.mlo_mobile_phone) as mlo_phone,
pup.data->'apsm'->>'managerName' as level_1_manager,
pup.data->'apsm'->>'email' as level_1_manager_email,
pup.data->'psm'->>'managerName' as level_2_manager_name,
pup.data->'psm'->>'email' as level_2_manager_email,
pup.data->'asm'->>'managerName' as level_3_manager_name,
pup.data->'asm'->>'email' as level_3_manager_email,
pup.data->'asm'->>'name' as level_3_manager_region,
pup.data->'rsm'->>'managerName' as level_4_manager_name,
pup.data->'rsm'->>'email' as level_4_manager_email,
pup.data->'rsm'->>'name' as level_4_manager_region,
pup.data->'dsm'->>'managerName' as level_5_manager_name,
pup.data->'dsm'->>'email' as level_5_manager_email,
pup.data->'dsm'->>'name' as level_5_manager_region,
ROW_NUMBER() over (PARTITION BY 
pup.data->>'nmlsid' order by pup.updated DESC) as row_id
{% endif %}
from {{ ref('partner_user_profiles') }} pup 
left join {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id 
left outer join (
            select aggregate_id, 
            min(PhoneNumber) as mlo_office_phone
            from (
                    {% if target.type == 'snowflake' %}
                    select aggregate_id,
                    fp.value:phoneType::VARCHAR as PhoneType,
                    fp.value:phoneNumber::VARCHAR as phoneNumber
                    from {{ ref('partner_user_profiles') }} pup,
                    lateral flatten(input => pup.phones) fp
                    {% else %}
                    select aggregate_id,
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
            from (
                {% if target.type == 'snowflake' %}
                select aggregate_id,
                fp.value:phoneType::VARCHAR as PhoneType,
                fp.value:phoneNumber::VARCHAR as phoneNumber
                from {{ ref('partner_user_profiles') }} pup,
                lateral flatten(input => pup.phones) fp
                {% else %}
                select aggregate_id,
                json_array_elements(phones)->>'phoneType' as PhoneType,
                json_array_elements(phones)->>'phoneNumber' as phoneNumber
                from {{ ref('partner_user_profiles') }}
                {% endif %}
                ) pop
                where lower(pop.Phonetype) = 'mobile'
                group by aggregate_id) mmp on mmp.aggregate_id = pup.aggregate_id
where pup.email like '%citizens%' and pur.role = 'MLO'
)
select * from hierarchy_cte
where row_id = 1



