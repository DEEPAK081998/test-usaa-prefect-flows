WITH leads_data_agent_cte AS (
{% if target.type == 'snowflake' %}
    select lead_id,
cast(cr.value:profileIds[0]::VARCHAR as {{ uuid_formatter() }}) as ld_hla_selected_agent_1,
cast(cr.value:profileIds[1]::VARCHAR as {{ uuid_formatter() }}) as ld_hla_selected_agent_2,
cast(cr.value:profileIds[2]::VARCHAR as {{ uuid_formatter() }}) as ld_hla_selected_agent_3
 from {{ ref('leads_data') }} ld,
 lateral flatten(input => ld.data:customRouting) cr
{% else %}
    select lead_id,
cast(json_array_elements(data->'customRouting')->'profileIds'->>0 as uuid) as ld_hla_selected_agent_1,
cast(json_array_elements(data->'customRouting')->'profileIds'->>1 as uuid) as ld_hla_selected_agent_2,
cast(json_array_elements(data->'customRouting')->'profileIds'->>2 as uuid) as ld_hla_selected_agent_3
 from {{ ref('leads_data') }}
{% endif %}
)
,
profiles_cte AS (
    select
    pup.aggregate_id,
    concat(pup.first_name,' ',pup.last_name) as agent_name,
    pup.brokerage_code,
    pup.email as agent_email
    from {{ ref('partner_user_profiles') }} pup
)

select ldac.*,
    pupa.agent_name as ld_hla_selected_agent1_name,
    pupa.brokerage_code as ld_hla_selected_agent1_brokerage_code,
    pupa.agent_email as ld_hla_selected_agent1_email,
    pupb.agent_name as ld_hla_selected_agent2_name,
    pupb.brokerage_code as ld_hla_selected_agent2_brokerage_code,
    pupb.agent_email as ld_hla_selected_agent2_email,
    pupc.agent_name as ld_hla_selected_agent3_name,
    pupc.brokerage_code as ld_hla_selected_agent3_brokerage_code,
    pupc.agent_email as ld_hla_selected_agent3_email
    from leads_data_agent_cte ldac
    left join profiles_cte pupa on pupa.aggregate_id = ldac.ld_hla_selected_agent_1
    left join profiles_cte pupb on pupb.aggregate_id = ldac.ld_hla_selected_agent_2
    left join profiles_cte pupc on pupc.aggregate_id = ldac.ld_hla_selected_agent_3