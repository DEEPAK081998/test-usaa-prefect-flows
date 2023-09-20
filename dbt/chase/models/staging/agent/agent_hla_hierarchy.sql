select 
    distinct pur.child_profile_id as agentAggregateID, 
    pupc.id as agentID, pupc.first_name as agentFirstName, 
    pupc.last_name as agentLastName, 
    purc.role as agentRole,
    pur.parent_profile_id as hlaagregateID, 
    pupp.id as hlaID, pupp.first_name as hlaFirstName, 
    pupp.last_name as hlaLastName, 
    purp.role as hlaRole,
    pupp.data->'division'->>'managerName' as divisionManager,
    pupp.data->'division'->>'name' as divisionName,
    pupp.email as HLAemail,
    pupp.phones->'phoneType'->'office'->>'phoneNumber' as HLAphone,
    pupp.data->>'nmlsid' as hlanmlsid,
    lm.childID as hlaIDcheck,lm.parentid as lmID, 
    lm.parentFirstName as lmFirstName, 
    lm.parentLastName as lmLastName,
    lm.parentEmail as lmEmail,
    slm.childID as lmIDcheck,slm.parentid as slmID, 
    slm.parentFirstName as slmFirstName, 
    slm.parentLastName as slmLastName,
    slm.parentEmail as slmEmail
from {{ ref('partner_user_relationships') }} pur 
JOIN
    {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id 
left outer JOIN
    {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id 
left outer join
    {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id 
left outer jOIN
    {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id 
left outer JOIN
    (select 
        pur.child_profile_id, 
        pupc.id as childID, 
        pupc.first_name as childFirstName, 
        pupc.last_name as childLastName, 
        purc.role as childRole,
        pur.parent_profile_id, 
        pupp.id as parentID, 
        pupp.first_name as parentFirstName, 
        pupp.last_name as parentLastName, 
        purp.role as parentRole,
        pupp.email as parentEmail
    from {{ ref('partner_user_relationships') }} pur 
    JOIN
        {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id 
    JOIN
        {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id 
    join
        {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id 
    JOIN
        {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id
    where purp.role = 'HLA_LEADER'
    AND purc.role = 'HLA'
    and pur.enabled = '1') lm on pupp.aggregate_id = lm.child_profile_id 
left outer JOIN
    (select 
        pur.child_profile_id, 
        pupc.id as childID, 
        pupc.first_name as childFirstName, 
        pupc.last_name as childLastName, 
        purc.role as childRole,
        pur.parent_profile_id, 
        pupp.id as parentID, 
        pupp.first_name as parentFirstName, 
        pupp.last_name as parentLastName, 
        purp.role as parentRole,
        pupp.email as parentEmail
    from {{ ref('partner_user_relationships') }} pur 
    JOIN
        {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id 
    JOIN
        {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id 
    join
        {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id 
    JOIN
        {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id
    where purp.role = 'HLA_SENIOR_LEADER'
    AND purc.role = 'HLA_LEADER'
    and pur.enabled = '1') Slm on LM.parent_profile_id = SLM.child_profile_id
where purp.role = 'HLA'
AND purc.role = 'AGENT'
and pur.enabled = '1'