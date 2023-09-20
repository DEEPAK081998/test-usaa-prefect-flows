--last updated: 7/28/22
{{
  config(
    enabled=false,
    )
}}
WITH afm AS(
select pup.aggregate_id, pup.first_name as AgentFirstName, pup.last_name as AgentLastName, pup.data::json->'profileInfo'->>'stateLicences' as statelicense,
concat(pup.first_name,' ',pup.last_name) as AgentName, concat(agentHLAAssignment.hlaFirstName,' ',agentHLAAssignment.hlaLastName) as HLAName,
concat(agentHLAAssignment.lmFirstName,' ',agentHLAAssignment.lmLastName) as LMName, concat(agentHLAAssignment.slmFirstName,' ',agentHLAAssignment.slmLastName) as SLMName,
agentHLAAssignment.divisionName, pup.brokerage_code, pup.verification_status, hlaID,
pup.data::json->>'caeLegacyAgent' as caeLegacyAgent_,pup.data::json->'brokerage'->>'brokerNetwork' as brokernetworkid,
pup.partner_id::uuid = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' as agentntworkid,
CASE
            WHEN pup.data::json->'brokerage'->>'brokerNetwork' = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' THEN 'BHHS'::text
            WHEN pup.data::json->'brokerage'->>'brokerNetwork' = 'e6ff474f-ecdf-4b6e-b45c-97b86914468a' THEN 'HS'::text
            WHEN pup.data::json->'brokerage'->>'brokerNetwork' = '0a746d45-4921-41e1-9fd2-06b5a6f176b7' THEN 'CAE'::text
            ELSE 'Other'::text
        END AS brokernetworkname,
        CASE
            WHEN pup.partner_id::uuid = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' THEN 'BHHS'::text
            WHEN pup.partner_id::uuid = 'e6ff474f-ecdf-4b6e-b45c-97b86914468a' THEN 'HS'::text
            WHEN pup.partner_id::uuid = '0a746d45-4921-41e1-9fd2-06b5a6f176b7' THEN 'CAE'::text
            ELSE 'Other'::text
        END AS agentnetworkname,
case when lower(pup.brokerage_code) = 'cs001' then 'Non-HSF'
                            when lower (pup.brokerage_code) = 'hs001' then 'HSAdmin'
                            when substring(pup.brokerage_code,3,1) = '2' then 'HSF'
                            when substring(pup.brokerage_code,3,1) = '3' then 'HSF'
                            when substring(pup.brokerage_code,3,1) = '7' then 'HSoA'
                            when substring(pup.brokerage_code,3,1) = '8' then 'Non-HSF'
                            when substring(pup.brokerage_code,3,1) = '9' then 'Non-HSF'
                        else 'None' end as RCbrokerageGroup,
case when amp.agentMobilePhone is null then aop.agentOfficePhone else amp.agentMobilePhone end as agentMobilePhone,
aop.agentOfficePhone,
pup.email,
REPLACE(CAST(pup.data::json->'referralAgreementEmail' AS TEXT),'"','') as referralAgreementEmail,
REPLACE(CAST(pup.data::json->'brokerage'->'email' AS TEXT), '"','') AS brokerage_email,
REPLACE(CAST(pup.data::json->'brokerage'->'phones'->0->'phoneNumber' AS TEXT), '"','') AS brokerage_phone,
CAST(pup.data::json->'inviter'->'email' AS TEXT) AS inviter_email,
CAST(pup.data::json->'caeLegacyAgent' AS TEXT) as caeLegacyAgent,
agentAcceptedCount.agentAcceptCount,
agentRejectCounts.agentRejectCount,
agentCreatedCounts.agentCreatedCount,
agentRoutedCounts.agentRoutedCount,
agentCTCCounts.agentCTCCount,
agentReferralCounts.referralCount,
agentTimeoutCounts.agentTimeoutCount,
agentStatusCounts.agentStatusCount,
agentClosedCounts.agentClosedCount,
Last30days_agentAcceptedCount.Last30days_agentAcceptCount,
Last30days_agentRejectCounts.Last30days_agentRejectCount,
Last30days_agentTimeoutCounts.Last30days_agentTimeoutCount,
Last30days_agentCreatedCounts.Last30days_agentCreatedCount,
Last30days_agentCTCCounts.Last30days_agentCTCCount,
Last90days_agentAcceptedCount.Last90days_agentAcceptCount,
Last90days_agentRejectCounts.Last90days_agentRejectCount,
Last90days_agentTimeoutCounts.Last90days_agentTimeoutCount,
Last90days_agentCreatedCounts.Last90days_agentCreatedCount,
Last90days_agentCTCCounts.Last90days_agentCTCCount,
ActiveStatus.ActiveStatus,
PendingStatus.PendingStatus,
ClosedStatus.ClosedStatus,
InactiveStatus.InactiveStatus,
OnholdStatus.OnholdStatus,
OfferAcceptedStatus.OfferAcceptedStatus,
ListingApptStatus.ListingApptStatus,
agentAcceptedCount.lastReferralDate,
case when pup.brokerage_code is null then pup.data::json->'brokerage'->>'name' else b.full_name end as brokerage_name,
rc.RC_Name,
rc.RC_Email,
rc.RC_Phone,
c.currentAssignments
from partner_user_profiles pup
    JOIN partner_user_roles pur on pur.user_profile_id = pup.id
    left join brokerages b on pup.brokerage_code = b.brokerage_code
/* twilio agent accepted count */
    left outer join (select profile_aggregate_id, count(lead_id) as agentAcceptCount, max(created) as lastReferralDate
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.accepted'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentAcceptedCount on agentAcceptedCount.profile_aggregate_id = pup.aggregate_id
/*twilio timeout count*/
     left outer join (select profile_aggregate_id, count (lead_id) as agentTimeoutCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.timeout'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentTimeoutCounts on agentTimeoutCounts.profile_aggregate_id = pup.aggregate_id
/* twilio agent rejected count */
    left outer join (select profile_aggregate_id, count (lead_id) as agentRejectCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.rejected'
                    AND lower(role) in ('agent','referral_coordinator')
                    group by profile_aggregate_id) agentRejectCounts on agentRejectCounts.profile_aggregate_id = pup.aggregate_id
/*twilio created count*/
     left outer join (select profile_aggregate_id, count (distinct lead_id) as agentCreatedCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.created'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentCreatedCounts on agentCreatedCounts.profile_aggregate_id = pup.aggregate_id
/*twilio routed count*/
     left outer join (select profile_aggregate_id, count (lead_id) as agentRoutedCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.created'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentRoutedCounts on agentRoutedCounts.profile_aggregate_id = pup.aggregate_id
/* twilio CTC completed*/
 left outer join (select profile_aggregate_id, count (lead_id) as agentCTCCount
                    from lead_status_updates
                    WHERE status LIKE 'Outreach Click to Call'
                    AND lower(role) = 'agent'
                    group by profile_aggregate_id) agentCTCCounts on agentCTCCounts.profile_aggregate_id = pup.aggregate_id
left outer join (select profile_aggregate_id, count(lead_id) as currentAssignments
                    from current_assignments 
                    where role = 'AGENT'
                    group by profile_aggregate_id) c on c.profile_aggregate_id = pup.aggregate_id
--Activity Counts for the last 30 days only
/* twilio agent accepted count */
    left outer join (select profile_aggregate_id, count(*) as Last30days_agentAcceptCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.accepted'
                    AND lower(role) ='agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentAcceptedCount on Last30days_agentAcceptedCount.profile_aggregate_id = pup.aggregate_id
/*twilio timeout count*/
     left outer join (select profile_aggregate_id, count (*) as Last30days_agentTimeoutCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.timeout'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentTimeoutCounts on Last30days_agentTimeoutCounts.profile_aggregate_id = pup.aggregate_id
/* twilio agent rejected count */
    left outer join (select profile_aggregate_id, count (*) as Last30days_agentRejectCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.rejected'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentRejectCounts on Last30days_agentRejectCounts.profile_aggregate_id = pup.aggregate_id
/*twilio created count*/
     left outer join (select profile_aggregate_id, count (*) as Last30days_agentCreatedCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.created'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentCreatedCounts on Last30days_agentCreatedCounts.profile_aggregate_id = pup.aggregate_id
/* twilio CTC completed*/
 left outer join (select profile_aggregate_id, count (*) as Last30days_agentCTCCount
                    from lead_status_updates
                    WHERE status LIKE 'Outreach Click to Call'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentCTCCounts on Last30days_agentCTCCounts.profile_aggregate_id = pup.aggregate_id
--end 30 days status counts

--Activity Counts for the last 90 days only
/* twilio agent accepted count */
    left outer join (select profile_aggregate_id, count(*) as Last90days_agentAcceptCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.accepted'
                    AND lower(role) ='agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentAcceptedCount on Last90days_agentAcceptedCount.profile_aggregate_id = pup.aggregate_id
/*twilio timeout count*/
     left outer join (select profile_aggregate_id, count (*) as Last90days_agentTimeoutCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.timeout'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentTimeoutCounts on Last90days_agentTimeoutCounts.profile_aggregate_id = pup.aggregate_id
/* twilio agent rejected count */
    left outer join (select profile_aggregate_id, count (*) as Last90days_agentRejectCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.rejected'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentRejectCounts on Last90days_agentRejectCounts.profile_aggregate_id = pup.aggregate_id
/*twilio created count*/
     left outer join (select profile_aggregate_id, count (*) as Last90days_agentCreatedCount
                    from lead_status_updates
                    WHERE data::json->>'twilioEvent' = 'reservation.created'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentCreatedCounts on Last90days_agentCreatedCounts.profile_aggregate_id = pup.aggregate_id
/* twilio CTC completed*/
 left outer join (select profile_aggregate_id, count (*) as Last90days_agentCTCCount
                    from lead_status_updates
                    WHERE status LIKE 'Outreach Click to Call'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentCTCCounts on Last90days_agentCTCCounts.profile_aggregate_id = pup.aggregate_id
--end 90 days status counts
/*Status Updates */
left outer join (select profile_aggregate_id, count (*) as agentStatusCount
                    from lead_status_updates
                    WHERE category like 'Property%'
                    and status not in ('New New Referral', 'Active Agent Assigned') 
                    and status not like ('Routing%')
                    and status not like ('Update%')
                    AND lower(role) = 'agent'
                    group by profile_aggregate_id) agentStatusCounts on agentstatusCounts.profile_aggregate_id = pup.aggregate_id
/*Referral Counts*/
left outer join(select profile_aggregate_id, count(lead_id) as referralCount
                    from current_assignments
                    where lower(role) = 'agent'
                    group by profile_aggregate_id) agentReferralCounts on agentReferralCounts.profile_aggregate_id = pup.aggregate_id
/*Closed Count*/
left outer join (select ca.profile_aggregate_id, count (cls.lead_id) as agentClosedCount
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Closed%'
                    group by ca.profile_aggregate_id) agentClosedCounts on agentClosedCounts.profile_aggregate_id = pup.aggregate_id
/*Current Active Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as ActiveStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Active%'
                    group by ca.profile_aggregate_id) ActiveStatus on ActiveStatus.profile_aggregate_id = pup.aggregate_id
/*Current Pending Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as PendingStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Pending%'
                    group by ca.profile_aggregate_id) PendingStatus on PendingStatus.profile_aggregate_id = pup.aggregate_id
/*Current Closed Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as ClosedStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Closed%'
                    group by ca.profile_aggregate_id) ClosedStatus on ClosedStatus.profile_aggregate_id = pup.aggregate_id
/*Current Inactive Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as InactiveStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Inactive%'
                    group by ca.profile_aggregate_id) InactiveStatus on InactiveStatus.profile_aggregate_id = pup.aggregate_id
/*Current On Hold Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as OnholdStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'On Hold%'
                    group by ca.profile_aggregate_id) OnholdStatus on Onholdstatus.profile_aggregate_id = pup.aggregate_id
/*Current Offer Accepted Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as OfferAcceptedStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Offer Accepted%'
                    group by ca.profile_aggregate_id) OfferAcceptedStatus on OfferAcceptedStatus.profile_aggregate_id = pup.aggregate_id
/*Current Listing Appointment Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as ListingApptStatus
                    from current_lead_statuses cls
                    join current_assignments ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Listing Appointment%%'
                    group by ca.profile_aggregate_id) ListingApptStatus on ListingApptStatus.profile_aggregate_id = pup.aggregate_id
/* Agent HLA Hierarchy*/
left outer join(select pur.child_profile_id as agentAggregateID, pupc.id as agentID, pupc.first_name as agentFirstName, 
               pupc.last_name as agentLastName, purc.role as agentRole,
                pur.parent_profile_id as hlaagregateID, pupp.id as hlaID, pupp.first_name as hlaFirstName, 
                pupp.last_name as hlaLastName, purp.role as hlaRole,
                pupp.data::json->'division'->>'managerName' as divisionManager,
                pupp.data::json->'division'->>'name' as divisionName,
                pupp.email as HLAemail,
                pupp.phone as HLAphone,
                pupp.data::json->>'nmlsid' as hlanmlsid,
                lm.childID as hlaIDcheck,lm.parentid as lmID, lm.parentFirstName as lmFirstName, lm.parentLastName as lmLastName,
                slm.childID as lmIDcheck,slm.parentid as slmID, slm.parentFirstName as slmFirstName, slm.parentLastName as slmLastName
        from partner_user_relationships pur JOIN
                partner_user_profiles pupc on pur.child_profile_id = pupc.aggregate_id JOIN
                partner_user_profiles pupp on pur.parent_profile_id = pupp.aggregate_id join
                partner_user_roles purc on pupc.id = purc.user_profile_id JOIN
                partner_user_roles purp on pupp.id = purp.user_profile_id left outer JOIN
                (select pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName, 
                        pupc.last_name as childLastName, purc.role as childRole,
                            pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                            pupp.last_name as parentLastName, purp.role as parentRole
                    from partner_user_relationships pur JOIN
                            partner_user_profiles pupc on pur.child_profile_id = pupc.aggregate_id JOIN
                            partner_user_profiles pupp on pur.parent_profile_id = pupp.aggregate_id join
                            partner_user_roles purc on pupc.id = purc.user_profile_id JOIN
                            partner_user_roles purp on pupp.id = purp.user_profile_id
                where purp.role = 'HLA_LEADER'
                AND purc.role = 'HLA'
                and pur.enabled = '1') lm on pupp.aggregate_id = lm.child_profile_id left outer JOIN
                (select pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName, 
                        pupc.last_name as childLastName, purc.role as childRole,
                            pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName, 
                            pupp.last_name as parentLastName, purp.role as parentRole
                    from partner_user_relationships pur JOIN
                            partner_user_profiles pupc on pur.child_profile_id = pupc.aggregate_id JOIN
                            partner_user_profiles pupp on pur.parent_profile_id = pupp.aggregate_id join
                            partner_user_roles purc on pupc.id = purc.user_profile_id JOIN
                            partner_user_roles purp on pupp.id = purp.user_profile_id
                where purp.role = 'HLA_SENIOR_LEADER'
                AND purc.role = 'HLA_LEADER'
                and pur.enabled = '1') Slm on LM.parent_profile_id = SLM.child_profile_id
    where purp.role = 'HLA'
    AND purc.role = 'AGENT'
    and pur.enabled = '1') agentHLAAssignment on agentHLAAssignment.agentAggregateID = pup.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as agentMobilePhone
                               from (select aggregate_id, json_array_elements(phones::json)->>'phoneType' as PhoneType,
                                            json_array_elements(phones::json)->>'phoneNumber' as phoneNumber
                                       from partner_user_profiles) pop
                                      where lower(pop.Phonetype) = 'mobilephone'
                                    group by aggregate_id)  amp on amp.aggregate_id = pup.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as agentOfficePhone
                               from (select aggregate_id, json_array_elements(phones::json)->>'phoneType' as PhoneType,
                                            json_array_elements(phones::json)->>'phoneNumber' as phoneNumber
                                       from partner_user_profiles) pop
                                      where lower(pop.Phonetype) = 'office'
                                    group by aggregate_id)  aop on aop.aggregate_id = pup.aggregate_id
 left outer join (select b.brokerage_code,
                        pur.id,
                         concat(pup.first_name,' ',pup.last_name) as RC_Name,
                         pup.first_name, pup.last_name,
                         pup.email as RC_Email,
                         pup.phones as RC_Phone
                         from brokerages b
                         join partner_user_relationships pur on b.aggregate_id = pur.parent_profile_id
                         join partner_user_profiles pup on pur.child_profile_id = pup.aggregate_id
                         where pur.enabled = '1'
                         ) rc on rc.id = pup.id
Where lower(pur.role) = 'agent'
),
add_legacy_flag AS(
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