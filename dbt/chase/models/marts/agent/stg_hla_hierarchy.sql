{{
  config(
    materialized = 'table',
    enabled=true
    )
}}
with lm_cte as (
    select pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName, 
                        pupc.last_name as childLastName, purc.role as childRole,
                            pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                            pupp.last_name as parentLastName, purp.role as parentRole
                    from {{ ref('partner_user_relationships') }}  pur 
                    JOIN {{ ref('partner_user_profiles') }}  pupc on pur.child_profile_id = pupc.aggregate_id 
                    JOIN {{ ref('partner_user_profiles') }}  pupp on pur.parent_profile_id = pupp.aggregate_id 
                    join {{ ref('partner_user_roles') }}  purc on pupc.id = purc.user_profile_id
                    JOIN {{ ref('partner_user_roles') }}  purp on pupp.id = purp.user_profile_id
                where purp.role = 'HLA_LEADER'
                AND purc.role = 'HLA'
                and pur.enabled = '1'
),
slm_cte AS (
    select pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName, 
                        pupc.last_name as childLastName, purc.role as childRole,
                            pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                            pupp.last_name as parentLastName, purp.role as parentRole
                    from {{ ref('partner_user_relationships') }}  pur
                    JOIN {{ ref('partner_user_profiles') }}  pupc on pur.child_profile_id = pupc.aggregate_id
                    JOIN {{ ref('partner_user_profiles') }}  pupp on pur.parent_profile_id = pupp.aggregate_id 
                    join {{ ref('partner_user_roles') }}  purc on pupc.id = purc.user_profile_id
                    JOIN {{ ref('partner_user_roles') }}  purp on pupp.id = purp.user_profile_id
                where purp.role = 'HLA_SENIOR_LEADER'
                AND purc.role = 'HLA_LEADER'
                and pur.enabled = '1'
)
select
    pur.child_profile_id AS agentAggregateID,
    pupc.id AS agentID,
    pupc.first_name AS agentFirstName,
    pupc.last_name AS agentLastName,
    purc.role AS agentRole,
    pur.parent_profile_id AS hlaagregateID,
    pupp.id AS hlaID,
    pupp.first_name AS hlaFirstName,
    pupp.last_name AS hlaLastName,
    purp.role AS hlaRole,
    pupp.data::json->'division'->>'managerName' AS divisionManager,
    pupp.data::json->'division'->>'name' AS divisionName,
    pupp.data::json->>'chaseStandardIdentifier' AS hlaSID,
    pupp.email AS hlaemail,
    pupp.phone AS hlaphone,
    pupp.data::json->>'nmlsid' AS hlanmlsid,
    lm.childID AS hlaIDcheck,
    lm.parentid AS lmID,
    lm.parentFirstName AS lmFirstName,
    lm.parentLastName AS lmLastName,
    slm.childID AS lmIDcheck,
    slm.parentid AS slmID,
    slm.parentFirstName AS slmFirstName,
    slm.parentLastName AS slmLastName
        from {{ ref('partner_user_relationships') }}  pur 
        JOIN {{ ref('partner_user_profiles') }}  pupc on pur.child_profile_id = pupc.aggregate_id
        JOIN {{ ref('partner_user_profiles') }}  pupp on pur.parent_profile_id = pupp.aggregate_id 
        join {{ ref('partner_user_roles') }}  purc on pupc.id = purc.user_profile_id 
        JOIN {{ ref('partner_user_roles') }}  purp on pupp.id = purp.user_profile_id 
        left outer JOIN lm_cte lm on pupp.aggregate_id = lm.child_profile_id 
        left outer JOIN slm_cte Slm on LM.parent_profile_id = SLM.child_profile_id
    where purp.role = 'HLA'
    AND purc.role = 'AGENT'
    and pur.enabled = '1'