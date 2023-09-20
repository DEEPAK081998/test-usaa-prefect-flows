WITH lm AS(
        SELECT 
                pur.child_profile_id, 
                pupc.id as childID, 
                pupc.first_name as childFirstName, 
                pupc.last_name as childLastName, purc.role as childRole,
                pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                pupp.last_name as parentLastName, purp.role as parentRole
        FROM
                {{ ref('partner_user_relationships') }} pur 
        JOIN
                {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id 
        JOIN
                {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id 
        JOIN
                {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id 
        JOIN
                {{ ref('partner_user_roles') }}  purp on pupp.id = purp.user_profile_id
        WHERE 
                purp.role = 'HLA_SENIOR_LEADER'
                AND purc.role = 'HLA_LEADER'
                and pur.enabled = '1'

),
hierarchy AS(
select distinct 
        pur.child_profile_id as hlaAggregateID, 
        pupc.id as hlaID, pupc.first_name as hlaFirstName, 
        pur.enabled,
        pupc.last_name as hlaLastName, 
        purc.role as hlaRole,
        pur.parent_profile_id as lmagregateID, 
        pupp.id as lmID, pupp.first_name as lmFirstName, 
        pupp.last_name as lmLastName, purp.role as lmRole,
        pupp.data->'division'->>'managerName' as divisionManager,
        pupp.data->'division'->>'name' as divisionName,
        lm.parent_profile_id as slmAggregateID,lm.parentid as slmID, 
        lm.parentFirstName as slmFirstName, lm.parentLastName as slmLastName,
        concat(pupp.last_name,', ',pupp.first_name) as LMFullName,
        concat(lm.parentFirstName,', ',lm.parentLastName) as SLMFullName
FROM  
        {{ ref('partner_user_relationships') }} pur 
JOIN
        {{ ref('partner_user_profiles') }}  pupc on pur.child_profile_id = pupc.aggregate_id 
left outer JOIN
        {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id 
left outer join
        {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id 
left outer JOIN
        {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id 
left outer JOIN
        lm on pupp.aggregate_id = lm.child_profile_id 
                    where purp.role = 'HLA_LEADER'
                    and purc.role = 'HLA'
                    and pur.enabled = '1')
SELECT * from hierarchy
                    