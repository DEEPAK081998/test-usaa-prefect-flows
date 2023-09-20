{{
  config(
    materialized = 'table',
    enabled=true
    )
}}
--EXPLAIN ANALYZE
WITH
lead_status_cte  AS (
    select
        profile_aggregate_id,
        case
            when nl.normalized_sell_location->>'zip' is null
            then nl.normalized_purchase_location->>'zip'
            else nl.normalized_sell_location->>'zip'
        end as Zip,
        count(case when data->>'twilioEvent' = 'reservation.accepted' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentAcceptCount,
        count(case when data->>'twilioEvent' = 'reservation.timeout' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentTimeoutCount,
        count(case when data->>'twilioEvent' = 'reservation.rejected' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentRejectCount,
        count(distinct case when data->>'twilioEvent' = 'reservation.created' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentCreatedCount,
        count(case when data->>'twilioEvent' = 'reservation.created' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentRoutedCount,
        count(case when status LIKE 'Outreach Click to Call' AND lower(role) = 'agent' THEN  lsu.lead_id END) as agentCTCCount,
        count(case when category like 'Property%' and status not in ('New New Referral', 'Active Agent Assigned') and status not like ('Routing%') and status not like ('Update%') AND lower(role) = 'agent' THEN  lsu.lead_id END) as agentStatusCount,
        max(case when data->>'twilioEvent' = 'reservation.accepted' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.created END) as lastReferralDate
    from {{ ref('lead_status_updates') }} lsu
    join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = lsu.lead_id
    group by 
        profile_aggregate_id,
        nl.normalized_sell_location->>'zip',
        nl.normalized_purchase_location->>'zip'
)
, lead_status_by_county_cte AS (
    SELECT 
        cte.profile_aggregate_id,
        cbsa_locations.county,
        cbsa_locations.st,
        SUM(agentAcceptCount) AS agentAcceptCount,
        SUM(agentTimeoutCount) AS agentTimeoutCount,
        SUM(agentRejectCount) AS agentRejectCount,
        SUM(agentCreatedCount) AS agentCreatedCount,
        SUM(agentRoutedCount) AS agentRoutedCount,
        SUM(agentCTCCount) AS agentCTCCount,
        SUM(agentStatusCount) AS agentStatusCount,
        MAX(lastReferralDate) AS lastReferralDate
    FROM lead_status_cte cte
    LEFT JOIN  {{ ref('cbsa_locations') }} cbsa_locations
    On cbsa_locations.zip = cte.zip
    GROUP BY 
        cte.profile_aggregate_id,
        cbsa_locations.county, 
        cbsa_locations.st
)
, current_assignments_cte AS (
    select 
        profile_aggregate_id, 
        case when nl.normalized_sell_location->>'zip' is null then nl.normalized_purchase_location->>'zip' else nl.normalized_sell_location->>'zip' end as Zip, count(ca.lead_id) as referralCount
    from {{ ref('current_assignments') }} ca
    join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = ca.lead_id
    where lower(role) = 'agent'
    group by 
        profile_aggregate_id,
        nl.normalized_sell_location->>'zip',
        nl.normalized_purchase_location->>'zip'
)
, current_assignments_by_county_cte AS (
    SELECT 
        cte.profile_aggregate_id,
        cbsa_locations.county,
        cbsa_locations.st,
        SUM(referralCount) as referralCount
    FROM current_assignments_cte  cte
    LEFT JOIN cbsa_locations 
    On cbsa_locations.zip = cte.zip
    GROUP BY 
        cte.profile_aggregate_id,
        cbsa_locations.county, 
        cbsa_locations.st
)
, current_lead_statuses_cte AS (
    select 
        ca.profile_aggregate_id,
        case
            when nl.normalized_sell_location->>'zip' is null then nl.normalized_purchase_location->>'zip' 
            else nl.normalized_sell_location->>'zip'
        end as Zip,
        count( case when category like 'Property%' and status like 'Closed%' then cls.lead_id end) as agentClosedCount,
        count( case when category like 'Property%' and status like 'Pending%' then cls.lead_id end) as PendingStatus,
        count( case when category like 'Property%' and status like 'Inactive%' then cls.lead_id end) as InactiveStatus,
        count( case when category like 'Property%' and status like 'On Hold%' then cls.lead_id end) as OnholdStatus,
        count( case when category like 'Property%' and status like 'Offer Accepted%' then cls.lead_id end) as OfferAcceptedStatus,
        count( case when category like 'Property%' and status like 'Active%' then cls.lead_id end) as ActiveStatus
    from {{ ref('current_lead_statuses') }} cls
    join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = cls.lead_id
    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
    group by 
        ca.profile_aggregate_id,
        nl.normalized_sell_location->>'zip',
        nl.normalized_purchase_location->>'zip'
)
, current_lead_statuses_by_county_cte AS (
    SELECT 
        cte.profile_aggregate_id,
        cbsa_locations.county,
        cbsa_locations.st,
        SUM(agentClosedCount) as agentClosedCount,
        SUM(PendingStatus) as PendingStatus,
        SUM(InactiveStatus) as InactiveStatus,
        SUM(OnholdStatus) as OnholdStatus,
        SUM(OfferAcceptedStatus) as OfferAcceptedStatus,
        SUM(ActiveStatus) as ActiveStatus
    FROM current_lead_statuses_cte cte
    LEFT JOIN {{ ref('cbsa_locations') }} cbsa_locations
    On cbsa_locations.zip = cte.zip
    GROUP BY cte.profile_aggregate_id,cbsa_locations.county, cbsa_locations.st
),
cbsa_cte AS (
    select 
        cbsa.st,
        cbsa.county,
        count(cbsa.zip) as CountyZipCount
    from {{ source('public', 'raw_cbsa_locations') }} cbsa
    group by 
        cbsa.st,
        cbsa.county
),
agent_county_coverage as (
    select
    pcz.profile_id,
    cbsa.st,
    cbsa.county,
    count(pcz.zip) as AgentCountyZips
    from {{ ref('profile_coverage_zips') }} pcz 
    join {{ ref('cbsa_locations') }} cbsa on lpad(cbsa.zip,5,'0') = pcz.zip
    group by 
        pcz.profile_id,
        cbsa.st,cbsa.county
), 
results_cte AS (
    select 
        cbsa_cte.st,
        cbsa_cte.county, 
        agent_county_coverage.profile_id,AgentCountyZips, 
        CountyZipCount,agentcountyzips::float/countyzipcount::float as Coverage  
    from 
        agent_county_coverage
    left join cbsa_cte
    on agent_county_coverage.st = cbsa_cte.st
    and agent_county_coverage.county = cbsa_cte.county
),
afm AS (
    select 
        pup.aggregate_id, pup.first_name as AgentFirstName, pup.last_name as AgentLastName, pup.data->'profileInfo'->>'stateLicences' as statelicense,
        concat(pup.first_name,' ',pup.last_name) as AgentName, pup.data->>'languages' as languages, concat(agentHLAAssignment.hlaFirstName,' ',agentHLAAssignment.hlaLastName) as HLAName,
        concat(agentHLAAssignment.lmFirstName,' ',agentHLAAssignment.lmLastName) as LMName, concat(agentHLAAssignment.slmFirstName,' ',agentHLAAssignment.slmLastName) as SLMName,
        agentHLAAssignment.divisionName, pup.brokerage_code, 
        case when pup.verification_status = 'Verified-Provisional' then 'Verified-Verified'
        when pup.verification_status = 'Verified-Select Only' then 'Verified-Verified'
        else pup.verification_status end as verification_status,
        pup.verification_status as internal_verificationstatus, hlaID,
        pup.data->>'caeLegacyAgent' as caeLegacyAgent_,pup.data->'brokerage'->>'brokerNetwork' as brokernetworkid,
        pup.partner_id::uuid = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' as agentntworkid,
        CASE
                    WHEN pup.data->'brokerage'->>'brokerNetwork' = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' THEN 'BHHS'::text
                    WHEN pup.data->'brokerage'->>'brokerNetwork' = 'e6ff474f-ecdf-4b6e-b45c-97b86914468a' THEN 'HS'::text
                    WHEN pup.data->'brokerage'->>'brokerNetwork' = '0a746d45-4921-41e1-9fd2-06b5a6f176b7' THEN 'CAE'::text
                    ELSE 'Other'::text
                END AS brokernetworkname,
                CASE
                    WHEN pup.partner_id::uuid = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' THEN 'BHHS'::text
                    WHEN pup.partner_id::uuid = 'e6ff474f-ecdf-4b6e-b45c-97b86914468a' THEN 'HS'::text
                    WHEN pup.partner_id::uuid = '0a746d45-4921-41e1-9fd2-06b5a6f176b7' THEN 'CAE'::text
                    ELSE 'Other'::text
                END AS agentnetworkname,
        case when amp.agentMobilePhone is null then aop.agentOfficePhone else amp.agentMobilePhone end as agentMobilePhone,
        aop.agentOfficePhone,
        pup.email,
        REPLACE(CAST(pup.data->'referralAgreementEmail' AS TEXT),'"','') as referralAgreementEmail,
        REPLACE(CAST(pup.data->'brokerage'->'email' AS TEXT), '"','') AS brokerage_email,
        REPLACE(CAST(pup.data->'brokerage'->'phones'->0->'phoneNumber' AS TEXT), '"','') AS brokerage_phone,
        CAST(pup.data->'inviter'->'email' AS TEXT) AS inviter_email,
        CAST(pup.data->'caeLegacyAgent' AS TEXT) as caeLegacyAgent,
        StatusCounts.agentAcceptCount,
        StatusCounts.agentRejectCount,
        StatusCounts.agentCreatedCount,
        StatusCounts.agentRoutedCount,
        StatusCounts.agentCTCCount,
        agentReferralCounts.referralCount,
        StatusCounts.agentTimeoutCount,
        StatusCounts.agentStatusCount,
        CurrentStatusCounts.agentClosedCount,
        CurrentStatusCounts.ActiveStatus,
        CurrentStatusCounts.PendingStatus,
        CurrentStatusCounts.agentClosedCount as ClosedStatus,
        CurrentStatusCounts.InactiveStatus,
        CurrentStatusCounts.OnholdStatus,
        CurrentStatusCounts.OfferAcceptedStatus,
        StatusCounts.lastReferralDate,
        case when pup.brokerage_code is null then pup.data->'brokerage'->>'name' else b.full_name end as brokerage_name,
        rc.RC_Name,
        rc.RC_Email,
        rc.RC_Phone,
        agentReferralCounts.referralCount as currentAssignments,
        StatusCounts.county,
        StatusCounts.st,
        cvr.CountyZipCount,
        cvr.AgentCountyZips,
        cvr.Coverage
    from {{ ref('partner_user_profiles') }} pup
    JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
    left join {{ ref('brokerages') }}  b on pup.brokerage_code = b.brokerage_code
    left outer join lead_status_by_county_cte StatusCounts on StatusCounts.profile_aggregate_id = pup.aggregate_id
    /*Referral Counts*/
    left outer join current_assignments_by_county_cte agentReferralCounts on agentReferralCounts.profile_aggregate_id = pup.aggregate_id and agentReferralCounts.county = StatusCounts.county and agentReferralCounts.st = StatusCounts.st
    left outer join current_lead_statuses_by_county_cte CurrentStatusCounts on CurrentStatusCounts.profile_aggregate_id = pup.aggregate_id and CurrentStatusCounts.county = StatusCounts.county and CurrentStatusCounts.st = StatusCounts.st
    left join results_cte cvr on cvr.st = StatusCounts.st and cvr.county=StatusCounts.county and cvr.profile_id=pup.id
    /* Agent HLA Hierarchy*/
    /* Agent HLA Hierarchy*/
    left outer join(select pur.child_profile_id as agentAggregateID, pupc.id as agentID, pupc.first_name as agentFirstName, 
               pupc.last_name as agentLastName, purc.role as agentRole,
                pur.parent_profile_id as hlaagregateID, pupp.id as hlaID, pupp.first_name as hlaFirstName, 
                pupp.last_name as hlaLastName, purp.role as hlaRole,
                pupp.data->'division'->>'managerName' as divisionManager,
                pupp.data->'division'->>'name' as divisionName,
                pupp.email as HLAemail,
                pupp.phone as HLAphone,
                pupp.data->>'nmlsid' as hlanmlsid,
                lm.childID as hlaIDcheck,lm.parentid as lmID, lm.parentFirstName as lmFirstName, lm.parentLastName as lmLastName,
                slm.childID as lmIDcheck,slm.parentid as slmID, slm.parentFirstName as slmFirstName, slm.parentLastName as slmLastName
        from {{ ref('partner_user_relationships') }} pur JOIN
                {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id JOIN
                {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id join
                {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id JOIN
                {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id left outer JOIN
                (select pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName, 
                        pupc.last_name as childLastName, purc.role as childRole,
                            pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                            pupp.last_name as parentLastName, purp.role as parentRole
                    from {{ ref('partner_user_relationships') }}  pur JOIN
                            {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id JOIN
                            {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id join
                            {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id JOIN
                            {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id
                where purp.role = 'HLA_LEADER'
                AND purc.role = 'HLA'
                and pur.enabled = '1') lm on pupp.aggregate_id = lm.child_profile_id 
            left outer JOIN
                (select pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName, 
                        pupc.last_name as childLastName, purc.role as childRole,
                            pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                            pupp.last_name as parentLastName, purp.role as parentRole
                    from {{ ref('partner_user_relationships') }} pur JOIN
                            {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id JOIN
                            {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id join
                            {{ ref('partner_user_roles') }} purc on pupc.id = purc.user_profile_id JOIN
                            {{ ref('partner_user_roles') }} purp on pupp.id = purp.user_profile_id
                where purp.role = 'HLA_SENIOR_LEADER'
                AND purc.role = 'HLA_LEADER'
                and pur.enabled = '1') Slm on LM.parent_profile_id = SLM.child_profile_id
    where purp.role = 'HLA'
    AND purc.role = 'AGENT'
    and pur.enabled = '1') agentHLAAssignment on agentHLAAssignment.agentAggregateID = pup.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as agentMobilePhone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from {{ ref('partner_user_profiles') }} ) pop
                                      where lower(pop.Phonetype) = 'mobilephone'
                                    group by aggregate_id)  amp on amp.aggregate_id = pup.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as agentOfficePhone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from {{ ref('partner_user_profiles') }} ) pop
                                      where lower(pop.Phonetype) = 'office'
                                    group by aggregate_id)  aop on aop.aggregate_id = pup.aggregate_id
    left outer join (select b.brokerage_code,
                        pur.id,
                         concat(pup.first_name,' ',pup.last_name) as RC_Name,
                         pup.first_name, pup.last_name,
                         pup.email as RC_Email,
                         pup.phones as RC_Phone
                         from {{ ref('brokerages') }} b
                         join {{ ref('partner_user_relationships') }} pur on b.aggregate_id = pur.parent_profile_id
                         join {{ ref('partner_user_profiles') }}  pup on pur.child_profile_id = pup.aggregate_id
                         where pur.enabled = '1'
                         ) rc on rc.id = pup.id
Where lower(pur.role) = 'agent' and pup.aggregate_id <>'00000000-0000-0000-0000-000000000000'
)
,add_legacy_flag AS(
  SELECT 
    *,
    CASE 
      WHEN caeLegacyAgent='true' THEN 'true' ELSE 'false' END as cae_legacy_agent,
    CASE 
      WHEN lower(inviter_email) LIKE '%chase%' AND (caeLegacyAgent IS NULL OR caeLegacyAgent='false') THEN 'true' ELSE 'false' END as hla_invited_agent,
    -- cae_grandfather (cae_legacy OR hla_invited_agent, OR hla affliation THEN true else false)
    CASE 
      WHEN hlaID IS NOT NULL THEN 'true' ELSE 'false' END as hla_affliated,
    CASE
      WHEN caeLegacyAgent='true' THEN 'true' 
      WHEN hlaID IS NOT NULL THEN 'true'
      WHEN lower(inviter_email) LIKE '%chase%' THEN 'true'
      ELSE 'false' END as cae_grandfather
    -- do they have an hla affliation- if the hla name is populated it's true
  FROM 
    afm
)
SELECT * FROM add_legacy_flag