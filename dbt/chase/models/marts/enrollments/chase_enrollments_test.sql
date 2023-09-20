{{
  config(
    materialized = 'table'
    )
}}
WITH rc_brokerage_details AS (
        SELECT
                ca.lead_id,
                b.id,
                b.brokerage_code,
                b.full_name,
                b.rc_name,
                b.first_name, 
                b.last_name,
                b.rc_email,
                b.rc_phone,
                b.rc_city,
                b.rc_state,
                case when lower(b.brokerage_code) = 'cs001' then 'CAE'
                   when lower (b.brokerage_code) = 'hs001' then 'HSAdmin'
                   when substring(b.brokerage_code,3,1) = '2' then 'RLRE'
                   when substring(b.brokerage_code,3,1) = '3' then 'BHHS'
                   when substring(b.brokerage_code,3,1) = '4' then 'HS'
                   when substring(b.brokerage_code,3,1) = '5' then 'RISE'
                   when substring(b.brokerage_code,3,1) = '7' then 'HSoA'
                   when substring(b.brokerage_code,3,1) = '8' then 'HS'
                   when substring(b.brokerage_code,3,1) = '9' then 'HS'
                else 'Other' end as RCbrokerageGroup
        FROM {{ ref('current_assignments') }} ca
        LEFT JOIN  {{ ref('brokerages') }} b
        on profile_aggregate_id = b.aggregate_id 
        where ca.role = 'BROKERAGE'
)

, pop_numbers_cte AS (
        SELECT aggregate_id, json_array_elements(phones::json)->>'phoneType' as PhoneType,json_array_elements(phones::json)->>'phoneNumber' as phoneNumber
        FROM {{ ref('partner_user_profiles') }}
)

, pop_mobile_number AS (
       SELECT aggregate_id, min(PhoneNumber) as agentMobilePhone
       FROM pop_numbers_cte pop
       WHERE lower(pop.Phonetype) = 'mobilephone'
       GROUP BY aggregate_id
)

, pop_office_number AS (
       SELECT aggregate_id, min(PhoneNumber) as agentOfficePhone
       FROM pop_numbers_cte pop
       WHERE lower(pop.Phonetype) = 'office'
       GROUP BY aggregate_id
)

, assignedAgentTotalReferrals_cte AS (
        SELECT profile_aggregate_id, count(distinct lead_id) as assignedAgentTotalReferrals
        FROM {{ ref('lead_status_updates') }}
        WHERE lower (role) = 'agent'
        GROUP BY profile_aggregate_id
)

, _current_assigned_agent_details_cte AS (
        SELECT
                ca.lead_id, ca.role, pup.email as email,
                pup.phones as phone,
                aop.agentOfficePhone,
                amp.agentMobilePhone,
                concat(pup.first_name,' ',pup.last_name) as fullName,
                pup.first_name as AgentFirstName,
                pup.last_name as AgentLastName,
                pup.brokerage_code as agentBrokerageCode,
                pup.verification_status as VerificationStatus,
                case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else pup.data->'brokerage'->>'name' end as agentBrokerageName,
                pup.aggregate_id as agentAggregateId,
                pup.data::json->'caeLegacyAgent' as caeLegacyAgent,
                pup.data::json->>'referralAgreementEmail' as ReferralEmail,
                pup.data::json->'brokerage'->>'name' as Brokerage,
                pup.data::json->'brokerage'->>'phones' as BrokeragePhone,
                pup.data::json->'brokerage'->>'email' as BrokerageEmail,
                concat(pup2.first_name,' ',pup2.last_name) as aRCName,
                case when lower(pup.brokerage_code) = 'cs001' then 'CAE'
                    when lower (pup.brokerage_code) = 'hs001' then 'HSAdmin'
                    when substring(pup.brokerage_code,3,1) = '2' then 'RLRE'
                    when substring(pup.brokerage_code,3,1) = '3' then 'BHHS'
                    when substring(pup.brokerage_code,3,1) = '7' then 'HSoA'
                    when substring(pup.brokerage_code,3,1) = '8' then 'HS'
                    when substring(pup.brokerage_code,3,1) = '9' then 'HS'
                else 'Other' end as agentbrokerageGroup,
                CASE
                    WHEN pup.partner_id = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' THEN 'BHHS'::text
                    WHEN pup.partner_id = 'e6ff474f-ecdf-4b6e-b45c-97b86914468a' THEN 'HS'::text
                    WHEN pup.partner_id = '0a746d45-4921-41e1-9fd2-06b5a6f176b7' THEN 'CAE'::text
                    ELSE 'Other'::text
                    END AS agentnetworkname,
                atr.assignedAgentTotalReferrals
        FROM {{ ref('current_assignments') }} ca
        JOIN {{ ref('partner_user_profiles') }} pup on ca.profile_aggregate_id = pup.aggregate_id
        LEFT JOIN {{ ref('brokerages') }} b on pup.brokerage_code = b.brokerage_code
        LEFT JOIN {{ ref('partner_user_relationships') }} pur on b.aggregate_id = pur.parent_profile_id
        /*left join brokerage_user bu on bu.brokerage_code = pup.brokerage_code*/
        LEFT JOIN {{ ref('partner_user_profiles') }} pup2 on pup2.aggregate_id = pur.child_profile_id
        LEFT JOIN pop_office_number aop on aop.aggregate_id = ca.profile_aggregate_id
        LEFT JOIN pop_mobile_number amp on amp.aggregate_id = ca.profile_aggregate_id
        LEFT JOIN assignedAgentTotalReferrals_cte atr on atr.profile_aggregate_id = ca.profile_aggregate_id
        WHERE ca.role='AGENT'
), 
DeduplicatedCTE AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY lead_id, agentbrokeragename ORDER BY lead_id) AS row_num
    FROM
        _current_assigned_agent_details_cte
),
current_assigned_agent_details_cte AS(
	select
		*
	from 
		DeduplicatedCTE
	where row_num =1 
),
totalInvitesbyLO_cte AS (
        select lop.hlaAggregateID, count(lop.lead_id) as totalInvitesbyLO
        from (
                select lead_id, ca.profile_aggregate_id as hlaAggregateID
                from {{ ref('current_assignments') }} ca
                where role = 'HLA'
                ) lop
        left outer join {{ ref('leads_data') }} ld on ld.lead_id = lop.lead_id
        where ld.data::json->>'mloSubmission' = 'true'
        group by hlaaggregateID
)
, hla_details_cte AS (
        select lead_id, ca.profile_aggregate_id as hlaAggregateID, pup.email, pup.phones as phone, pup.first_name as HLA_first ,pup.last_name as HLA_last,
                concat(pup.first_name,' ',pup.last_name) as fullName, pup.data::json->'nmlsid' as NMLSid, pup.data::json->>'chaseStandardIdentifier' as hlaSID,
                lop2.totalInvitesbyLO
        from {{ ref('current_assignments') }} ca
        join {{ ref('partner_user_profiles') }} pup on ca.profile_aggregate_id = pup.aggregate_id
        left outer join totalInvitesbyLO_cte lop2 on ca.profile_aggregate_id = lop2.hlaAggregateID
        where role = 'HLA'
)

, hla_assistant_details_cte AS (
        SELECT lead_id, lsu.data, pup.first_name as HSA_first_name, pup.last_name as HSA_last_name, concat(pup.first_name,' ',pup.last_name) as HSAfullname, (lsu.data::json->'invitedBy'->>'aggregateId')::uuid as HSAaggregateid
        FROM {{ ref('lead_status_updates') }} lsu
        LEFT JOIN {{ ref('partner_user_profiles') }} pup on (lsu.data::json->'invitedBy'->>'aggregateId')::text = pup.aggregate_id::text
        WHERE lsu.role = 'HLA' and lower(status) = 'invited unaccepted'
)
/* Last Agent Updates */
, last_agent_update_cte AS (
        SELECT lead_id, max(created) as last_agent_update
        FROM {{ ref('lead_status_updates') }}
        where role = 'AGENT' and category in ('PropertySell','PropertySearch')
        group by lead_id
)
/* inactive date from lead status updates */

, inactive_status_update_cte_part AS (
        SELECT distinct on (lead_id) lead_id, min(created) as inactiveDate, role as inactiveRole
        FROM {{ ref('lead_status_updates') }} ls
        WHERE category in ('PropertySell','PropertySearch','ConciergeStatus') and status like 'Inactive%'
        group by lead_id, role
)

, inactive_status_update_cte AS (
        SELECT l.id, ind.inactiveDate, ind.inactiveRole,
                DATE_PART('day', ind.inactiveDate - l.created) as timetoInactive
        FROM {{ ref('leads') }} l
        LEFT join inactive_status_update_cte_part ind on l.id = ind.lead_id
)

{#-
Lead Status update cte, there was multiple joins to this table even pulling the same info includes:
First Contact time using status = Outreach Click to Call, role = agent or RC and profile is not admin
Last network Contact time using status = Outreach Click to Call, role = agent or RC and profile is not admin
Last network Contact time using status = Outreach Click to Call, role = agent profile is not admin
Count of lead assignments for a customer excluding admin
First accepted time by an Agent
First assignment to an agent
Enrollment date - first status not like Invite% with a catory like Property% 
escalated to Concierge
zero agents covering the area
low agent coverage
agent direct timeouts
agent select timeouts
timeout counts
routing attempts
-#}
, lead_status_updates_cte AS (
        SELECT
                lead_id,
                MIN(CASE WHEN category = 'Activities' and  status = 'Outreach Click to Call' and role in ('AGENT','REFERRAL_COORDINATOR') and profile_aggregate_id <> '81fbb29e-d690-40c0-8035-9aedc659fad5' THEN created end) as first_contact_time,
                MIN(CASE WHEN category = 'Activities' and  status = 'Outreach Click to Call' and role in ('AGENT','REFERRAL_COORDINATOR') and profile_aggregate_id <> '81fbb29e-d690-40c0-8035-9aedc659fad5' THEN created end) as first_contact_time_unadjusted,
                MAX(CASE WHEN category = 'Activities' and  status = 'Outreach Click to Call' and role in ('AGENT','REFERRAL_COORDINATOR') and profile_aggregate_id <> '81fbb29e-d690-40c0-8035-9aedc659fad5' THEN created end) as lastContactDate,
                COUNT(CASE WHEN category in ('PropertySell','PropertySearch') and  lower(status) = 'routing waiting to accept' and role = 'AGENT' and profile_aggregate_id <> '81fbb29e-d690-40c0-8035-9aedc659fad5' THEN 1 end) as leadAssignmentCount,
                MIN(CASE WHEN category in ('PropertySell','PropertySearch') and  lower(status) = 'active agent assigned' and role = 'AGENT' THEN created end) as agentAcceptTime,
                MIN(CASE WHEN category in ('PropertySell','PropertySearch') and  lower(status) like 'routing%' and role = 'AGENT' THEN created end) as firstAssignTime,
                MIN(CASE WHEN category in ('PropertySell','PropertySearch') and  lower(status) not like ('invite%') THEN created end) as enrollDate,
                COUNT(CASE WHEN data::json->>'twilioEvent' = 'reservation.rejected' and role = 'AGENT' THEN 1 end) as rejectedCount,
                MIN(CASE WHEN category in ('PropertySell','PropertySearch') and lower(status) like ('invite%') THEN created end) as inviteDate,
                MAX(CASE WHEN category like 'Property%' and status = 'Pending Offer Accepted' THEN created end) as lastPendingDate,
                MIN(CASE WHEN status = 'Routing Waiting Agent Selection' OR status = 'New Waiting Agent Selection' THEN created end) as firstAcceptedDate,
                MIN(CASE WHEN lower(status) = 'outreach connection confirmed' and lower(role) = 'admin' THEN created end) as firstConfirmedDate,
                MIN(CASE WHEN lower(status) = 'outreach text' OR lower(status) = 'outreach email' and lower(role) = 'admin' THEN created end) as firstRewardsTeamContactDate,
                MIN(CASE WHEN  status = 'Routing Ready to Route' THEN created end) as firstRoutedDate,
                MAX(CASE WHEN  data::json->>'twilioEvent' = 'reservation.created' THEN created end) as lastofferedDate,
                COUNT(CASE WHEN  status = 'Active Agent Assigned' and category in ('PropertySell','PropertySearch') and profile_aggregate_id <> '81fbb29e-d690-40c0-8035-9aedc659fad5' and role = 'AGENT' THEN 1 end) as ReassignmentCount,
                COUNT(CASE WHEN  data::json->>'twilioEvent' = 'reservation.accepted' AND role='AGENT' THEN 1 end) as agentAcceptCount,
                COUNT(CASE WHEN  data::json->>'twilioEvent' = 'reservation.created'  AND lower(role) = 'agent' THEN 1 end) as agentCreatedCount,
                COUNT(CASE WHEN profile_aggregate_id = '81fbb29e-d690-40c0-8035-9aedc659fad5' and data::json->>'twilioEvent' = 'reservation.created' THEN 1 END) as EscalatedtoConcierge,
                COUNT(CASE WHEN status like '%Ready to Route%' and json_array_length(data::json->'profileIds') in (1,2) and data::json->>'routingMethod' = 'AGENT_DIRECT' THEN 1 END) as LowCoverage,
                COUNT(CASE WHEN data::json->>'twilioEvent' = 'reservation.timeout' AND lower(role) = 'agent' and data::json->>'routingMethod' = 'AGENT_DIRECT' THEN 1 END) as agentdirectTimeoutCount,
                COUNT(CASE WHEN data::json->>'twilioEvent' = 'reservation.timeout' AND lower(role) = 'agent' and data::json->>'routingMethod' = 'AGENT_SELECT' THEN 1 END) as agentselectTimeoutCount,
                COUNT(CASE WHEN data::json->>'twilioEvent' = 'reservation.timeout' AND lower(role) = 'agent' THEN 1 END) as agentTimeoutCount,
                COUNT(CASE WHEN status like '%Ready to Route%' and data::json->>'routingMethod' = 'AGENT_DIRECT' or data::json->>'routingMethod' = 'AGENT_SELECT' THEN 1 END) as routingattempts

        FROM {{ ref('lead_status_updates') }}
        GROUP BY lead_id
)
/* RC, Agent, HLA assignment times. This is legacy from HS system */

, ua1_cte AS (
        SELECT
                lead_id,
                (min(case when role = 'REFERRAL_COORDINATOR' then created end) - interval '5 hour') as RC_assign_time,
                (min(case when role = 'AGENT' then created end) - interval '5 hour') as agent_assign_time,
                (min(case when role = 'AGENT' then created end)) as agent_assign_time_unadjusted,
                (min(case when role = 'HLA' then created end) - interval '5 hour') as HLA_assign_time
        FROM {{ ref('profile_assignments') }}
        group by lead_id
)
/* hlccsubmitted flag */
, hlccsubmitted as(
 select distinct
 lead_id as leadid,
 case 
  when ld.data::json->>'hlccSubmitted' = 'true' then 'true'
  else NULL end as hlcc_submitted
 from {{ ref('leads_data') }} ld)

, leads_on_profileassigment_cte as (
        SELECT
                DISTINCT lead_id 
        FROM {{ ref('profile_assignments') }} 
        WHERE profile_aggregate_id = '81fbb29e-d690-40c0-8035-9aedc659fad5'
)

, ZeroCoverage_cte AS (
        SELECT
                lead_id, count (*) as ZeroCoverage 
        FROM {{ ref('lead_status_updates') }}
        where status like '%Ready to Route%' and json_array_length(data::json->'profileIds') = 0
              AND lead_id IN (SELECT LEAD_ID FROM leads_on_profileassigment_cte)
        group by lead_id
)
, leads_created_routing_cte AS (
        SELECT lead_id, min(created) as created, max(created) as max_created
        FROM {{ ref('lead_status_updates') }}
        WHERE category in ('PropertySell','PropertySearch') and data::json->'routingMethod' is not null
        GROUP BY lead_id
)
, lrm_cte AS (
        SELECT l.lead_id, data::json->>'routingMethod' as routingMethod
        FROM {{ ref('lead_status_updates') }} l 
        JOIN leads_created_routing_cte a on l.lead_id = a.lead_id and l.created = a.max_created
        order by lead_id
)
, olrm_cte AS (
        SELECT l.lead_id, data::json->>'routingMethod' as originalroutingMethod
        FROM {{ ref('lead_status_updates') }} l 
        JOIN leads_created_routing_cte a on l.lead_id = a.lead_id and l.created = a.created
        order by lead_id
)
/* rewards team member assigned OLD Activity model */
, lc_for_rtm_cte AS (
        select lead_id, max(id) as id
        from {{ ref('lead_status_updates') }}
        where category = 'Activities' and role = 'ADMIN' AND lower(status) = 'claim claimed'
        group by lead_id
)
, rtm_cte AS (
        SELECT lsu.lead_id, concat(pup.first_name,' ', pup.last_name) as rewardsTeam, (lsu.created - interval '5 Hours') as claimDate
        FROM {{ ref('lead_status_updates') }} lsu
        JOIN {{ ref('partner_user_profiles') }} pup on lsu.profile_aggregate_id = pup.aggregate_id
        JOIN lc_for_rtm_cte lc on lc.id = lsu.id
)
/* rewards team member assigned NEW Assignment model */
, pa_for_rtmn_cte AS (
        SELECT lead_id, max(id) as assignmentID
        FROM {{ ref('profile_assignments') }} 
        WHERE role = 'ADMIN'
        GROUP BY lead_id
)
, rtmn_cte AS (
        SELECT ca.lead_id, concat(pup.first_name,' ',pup.last_name) as rewardsTeamNew, (ca.created - interval '5 Hours') as claimDateNew
        FROM pa_for_rtmn_cte pa 
        JOIN {{ ref('profile_assignments') }} ca on pa.lead_id = ca.lead_id and pa.assignmentID = ca.id
        JOIN {{ ref('partner_user_profiles') }} pup on ca.profile_aggregate_id = pup.aggregate_id
        WHERE lower(ca.role) = 'admin'
)
/* HLA, including Division, SLM and LM hierarchy _START */
, pur_enabled_cte AS (
        SELECT * FROM {{ ref('partner_user_roles') }} WHERE enabled
)
, lm_cte AS (
        SELECT
                pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName,
                pupc.last_name as childLastName, purc.role as childRole,
                pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName,
                pupp.last_name as parentLastName, purp.role as parentRole
        FROM {{ ref('partner_user_relationships') }} pur
        JOIN {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id
        JOIN {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id
        join pur_enabled_cte purc on pupc.id = purc.user_profile_id
        JOIN pur_enabled_cte purp on pupp.id = purp.user_profile_id
        WHERE purp.role = 'HLA_LEADER' AND purc.role = 'HLA' AND pur.enabled = '1'
)
, slm_cte AS (
        SELECT
                pur.child_profile_id, pupc.id as childID, pupc.first_name as childFirstName,
                pupc.last_name as childLastName, purc.role as childRole,
                pur.parent_profile_id, pupp.id as parentID, pupp.first_name as parentFirstName,
                pupp.last_name as parentLastName, purp.role as parentRole, pupp.email as parentEmail,
                ROW_NUMBER() over(partition by pur.child_profile_id order by pur.updated DESC) order_cte
        FROM {{ ref('partner_user_relationships') }} pur
        JOIN {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id
        JOIN {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id
        JOIN pur_enabled_cte purc on pupc.id = purc.user_profile_id
        JOIN pur_enabled_cte purp on pupp.id = purp.user_profile_id
        WHERE purp.role = 'HLA_SENIOR_LEADER' AND purc.role = 'HLA_LEADER' and pur.enabled = '1'
)

, hlah_cte AS (
        SELECT
                pur.child_profile_id as hlaAggregateID, pupc.id as hlaID, pupc.first_name as hlaFirstName, pur.enabled,
                pupc.last_name as hlaLastName, purc.role as hlaRole,
                pur.parent_profile_id as lmagregateID, pupp.id as lmID, pupp.first_name as lmFirstName, pupp.email as lmEmail,
                pupp.last_name as lmLastName, purp.role as lmRole,
                pupp.data::json->'division'->>'managerName' as divisionManager,
                pupp.data::json->'division'->>'name' as divisionName,
                lm.parent_profile_id as slmAggregateID,lm.parentid as slmID, lm.parentFirstName as slmFirstName, lm.parentLastName as slmLastName,
                lm.parentemail as slmEmail,
                concat(pupp.last_name,', ',pupp.first_name) as LMFullName,
                concat(lm.parentFirstName,', ',lm.parentFirstName) as SLMFullName,
                ROW_NUMBER() over(partition by pur.child_profile_id order by pur.updated DESC) order_cte_hla
        FROM {{ ref('partner_user_relationships') }} pur 
        JOIN {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id 
        LEFT JOIN {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id 
        LEFT JOIN pur_enabled_cte purc on pupc.id = purc.user_profile_id 
        LEFT JOIN pur_enabled_cte purp on pupp.id = purp.user_profile_id 
        LEFT JOIN slm_cte lm on pupp.aggregate_id = lm.child_profile_id and lm.order_cte = 1
        WHERE purp.role = 'HLA_LEADER' AND purc.role = 'HLA' AND pur.enabled = '1'
)
, agentHLAAssignment_cte AS (
        SELECT
                pur.child_profile_id as agentAggregateID,
                pupc.id as agentID,
                pupc.first_name as agentFirstName,
                pupc.last_name as agentLastName,
                purc.role as agentRole,
                pur.parent_profile_id as hlaagregateID,
                pupp.id as hlaID,
                pupp.first_name as hlaFirstName,
                pupp.last_name as hlaLastName,
                purp.role as hlaRole,
                pupp.data::json->'division'->>'managerName' as divisionManager,
                pupp.data::json->'division'->>'name' as divisionName,
                pupp.email as HLAemail,
                pupp.phone as HLAphone,
                pupp.data::json->>'nmlsid' as hlanmlsid,
                lm.childID as hlaIDcheck,
                lm.parentid as lmID,
                lm.parentFirstName as lmFirstName,
                lm.parentLastName as lmLastName,
                slm.childID as lmIDcheck,
                slm.parentid as slmID,
                slm.parentFirstName as slmFirstName,
                slm.parentLastName as slmLastName,
                row_number() over(partition by pur.child_profile_id order by pupp.updated DESC) order_Cte 
        FROM {{ ref('partner_user_relationships') }}  pur
        JOIN {{ ref('partner_user_profiles') }} pupc on pur.child_profile_id = pupc.aggregate_id
        JOIN {{ ref('partner_user_profiles') }} pupp on pur.parent_profile_id = pupp.aggregate_id
        JOIN pur_enabled_cte purc on pupc.id = purc.user_profile_id
        JOIN pur_enabled_cte purp on pupp.id = purp.user_profile_id
        LEFT JOIN lm_cte lm on pupp.aggregate_id = lm.child_profile_id
        LEFT JOIN slm_cte Slm on LM.parent_profile_id = SLM.child_profile_id  and slm.order_cte = 1
        WHERE purp.role = 'HLA' AND purc.role = 'AGENT' AND pur.enabled = '1'

)
/* HLA, including Division, SLM and LM hierarchy_END */

/* Last status updates by role and closing details  */

, cls_cte AS (
select a.lead_id,
                a.HLA_Status,
                a.HLA_Status_Time,
                a.RC_Status,
                a.RC_Status_Time,
                a.Agent_Status,
                a.Agent_Status_Time,
                hb.HB_Status,
                hb.HB_Status_time,
                a.Concierge_Status,
                a.Concierge_Status_Time,
                b.CloseDate,
                b.PropertyAddress,
                b.PropertyPrice,
                b.LenderClosedWith,
                b.AgtCommPct,
                c.PendingCloseDate,
                c.PendingPropertyAddress,
                d.FinanceCloseDate
                from (select lead_id,
                        max(case when role = 'HLA' then status end) as HLA_Status,
                        (max(case when role = 'HLA' then created end)-interval '5 hour') as HLA_Status_Time,
                        max(case when role = 'REFERRAL_COORDINATOR' and category in ('PropertySell','PropertySearch') then status end) as RC_Status,
                        (max(case when role = 'REFERRAL_COORDINATOR' and category in ('PropertySell','PropertySearch') then created end)-interval '5 hour') as RC_Status_Time,
                        max(case when role = 'AGENT' and category in ('PropertySell','PropertySearch') then status end) as Agent_Status,
                        (max(case when role = 'AGENT' and category in ('PropertySell','PropertySearch') then created end)-interval '5 hour') as Agent_Status_Time,
                        max(case when role = 'ADMIN' then status end) as Concierge_Status,
                        (max(case when role = 'ADMIN' then created end)-interval '5 hour') as Concierge_Status_Time
                        from {{ ref('current_lead_statuses') }}
                        group by lead_id) a
                left outer join (select cs.lead_id, cs.status as HB_Status, (cs.created-interval '5 hour') as HB_Status_Time
                                from {{ ref('current_lead_statuses') }} cs 
                                join (select distinct on (lead_id) id, lead_id
                                        from {{ ref('current_lead_statuses') }}
                                        where category in ('PropertySell','PropertySearch')
                                        order by lead_id, updated desc) mid on cs.id = mid.id) hb on a.lead_id = hb.lead_id
                left outer join (select  l.lead_id, data::json->>'closeDate' as PendingCloseDate,
                        l.data::json->>'propertyAddress' as PendingPropertyAddress
                        from {{ ref('lead_status_updates') }} l
                        JOIN
                        (select lead_id, max(id) as id
                        from {{ ref('lead_status_updates') }}
                        where data::json->'closeDate' is not null 
                        and category in ('PropertySell','PropertySearch') and status like 'Pending%'--and data::json->'salePrice' is not null
                        group by lead_id) n on l.lead_id = n.lead_id and l.id = n.id
                        ) c on c.lead_id = a.lead_id
                left outer join (
                        select l.lead_id, l.data::json->>'closeDate' as FinanceCloseDate
                        from {{ ref('lead_status_updates') }} l
                        join 
                        (select  lead_id, max(id) as id
                        from {{ ref('lead_status_updates') }}
                        where data::json->'closeDate' is not null 
                        and category in ('HomeFinancing') and status = 'Closed Closed'
                        group by lead_id) m on l.lead_id = m.lead_id and l.id = m.id
                        ) d on d.lead_id = a.lead_id
                left outer join (select  l.lead_id, data::json->>'closeDate' as CloseDate,
                        data::json->>'propertyAddress' as PropertyAddress,
                        data::json->>'salePrice' as PropertyPrice,
                        case
                            when lower(data::json->>'lender') like '%chase%' then 'Chase'
                            when lower(data::json->>'lender') like '%jpmc%' then 'Chase'
                            when lower(data::json->>'lender') like '%jpmorgan%' then 'Chase'
                            when lower(data::json->>'lender') like '%?%' then 'Unknown'
                            else data::json->>'lender' end as LenderClosedWith,
                        data::json->'agentCommissionPercentage' as AgtCommPct
                        from {{ ref('lead_status_updates') }} l
                        JOIN
                        (select lead_id, max(id) as id
                        from {{ ref('lead_status_updates') }}
                        where data::json->'closeDate' is not null 
                        and category in ('PropertySell','PropertySearch') and status = 'Closed Closed'--and data::json->'salePrice' is not null
                        group by lead_id) n on l.lead_id = n.lead_id and l.id = n.id
                        ) b on b.lead_id = a.lead_id
)
, total_assignments_cte AS (
        SELECT pa.lead_id, COUNT(DISTINCT(pa.profile_aggregate_id)) as total_assignments
        FROM {{ ref('profile_assignments') }} pa
        WHERE  (pa.role = 'AGENT' OR pa.role = 'REFERRAL_COORDINATOR')
        GROUP BY pa.lead_id
)
, odp_leads_cte AS (
  SELECT 
    nu.lead_id,
    true AS is_odp,
    ROW_NUMBER() OVER (PARTITION BY nu.lead_id ORDER BY nu.created DESC) AS row_num
  FROM {{ ref('note_updates') }} nu
  {% if target.type == 'snowflake' %}
    WHERE nu.data:newContent::VARCHAR ILIKE '%odp%'
  {% else %}
    WHERE nu.data->>'newContent'::VARCHAR ILIKE '%odp%'
  {% endif %}
)
, odp_tag_cte AS (
  {% if target.type == 'snowflake' %}
    SELECT 
      nu.lead_id,
      REGEXP_SUBSTR(nu.data:newContent::VARCHAR, '\\d{1,2}', 1, 1, 'e')::INTEGER as ODP_agentcount,
      REGEXP_SUBSTR(nu.data:newContent::VARCHAR, ':(\\w{1})', 1, 2, 'e')::VARCHAR as ODP_agenttype,
      ROW_NUMBER() OVER (PARTITION BY nu.lead_id ORDER BY nu.created DESC) AS row_num
    FROM {{ ref('note_updates') }} nu
    WHERE REGEXP_LIKE(nu.data:newContent::VARCHAR, '.*ODP:\\d{1,2}:(E|N).*')
  {% else %}
    SELECT 
      nu.lead_id,
      TRANSLATE(REGEXP_MATCH(nu.data->>'newContent', 'ODP:(\d{1,2}):[E|N]')::VARCHAR, '{}', '')::INTEGER as ODP_agentcount,
      TRANSLATE(REGEXP_MATCH(nu.data->>'newContent', 'ODP:\d{1,2}:(E|N)')::VARCHAR, '{}', '')::VARCHAR as ODP_agenttype,
      ROW_NUMBER() OVER (PARTITION BY nu.lead_id ORDER BY nu.created DESC) AS row_num
    FROM {{ ref('note_updates') }} nu
    WHERE nu.data->>'newContent' SIMILAR TO '%ODP:\d{1,2}:(E|N)%'
  {% endif %}
)
,
final_enrollments_cte AS (
--Chase Enrollments
select date(t1.created - interval '5 Hours'),
case when t1.consumer_confirmed = '0' then null else (ctc.enrollDate - interval '3 Hours') end as enrolledDate,
(ctc.inviteDate - interval '5 Hours') as invitedDate,
t1.id,
concat(t1.first_name,' ',t1.last_name) as client_name,
t1.email as client_email,
replace(t1.phone,'+','') as client_phone,
t1.contact_methods as client_contact_methods,
t1.bank_id as bank_id,
slb.bank_name as bank_name,
t2.fullName as agent_name,
t2.email as agent_email,
t2.phone as agent_phone,
t2.agentOfficePhone,
t2.agentMobilePhone,
t3.fullName as lo_name,
t3.email as lo_email,
t3.phone as lo_phone,
t3.hlaAggregateID,
hlah.hlaAggregateID as hlaCheck,
hlah.divisionManager,
hlah.divisionName,
concat(hlah.lmLastName,', ',hlah.lmFirstName) as LMFullName,
concat(hlah.slmLastName,', ',hlah.slmFirstName) as SLMFullName,
hlah.slmAggregateID,
hlah.lmagregateID,
t1.purchase_location,
case when t1.purchase_time_frame = 1 then 90
when t1.purchase_time_frame = 2 then 180 else 365 end as purchase_time_frame,
t1.prequal,
t1.comments,
t5.last_agent_update,
inds.inactivedate,
case when t1.transaction_type = 'PURCHASE' then 'BUY'
    when t1.transaction_type = 'BOTH' then 'BUY'
    else t1.transaction_type end as transaction_type,
t1.chase_id,
case 
when ld.data::json->>'mloSubmission' = 'true' then 'HLA invited' 
when ld.data::json->>'hlccSubmitted' = 'true' then 'HLA invited'
when ctc.invitedate is not null then 'HLA invited' 
else 'Customer Enrolled' end as enrollmentType,
t1.loan_details_sharing_enabled,
t1.consumer_confirmed,
t1.referral_fee_transaction,
--ld.data::json->>'ECI' as customerECI,
coalesce(ld.data::json->>'ECI',ld.data::json->>'eci') as customerECI,
ld.data::json->'fthb' as customerFTHB,
ld.data::json->>'languages' as customerLanguage,
ld.data::json->>'mloSubmission' as HLAEntered,
ld.data::json->>'reducedForm' as LOEnrollmentForm,
case when ld.data::json->'trafficSource'->>'medium' = 'sms' then 'SMS' else 'Email' end as inviteMedium,
case when ld.data::json->'trafficSource'->>'medium' = 'sms' then ld.created else null end as smsdate,
ld.data->>'smsInvitePermission' AS smspermission,
ld.data->'trafficSource'->>'source' AS smssource,
ld.data->'trafficSource'->>'campaign' AS campaign,
case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'zip' else nl.normalized_purchase_location::json->>'zip' end as normalizedZip,
case when t1.transaction_type = 'SELL' then concat('Z',nl.normalized_sell_location::json->>'zip') else concat('Z',nl.normalized_purchase_location::json->>'zip') end as stringZip,
case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'city' else nl.normalized_purchase_location::json->>'city' end as normalizedCity,
case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'state' else nl.normalized_purchase_location::json->>'state' end as normalizedState,
cls.HLA_Status as financingStatus,
cls.HLA_Status_time as financingStatusTime,
cls.HB_Status as realEstateStatus,
cls.HB_Status_Time as realEstateStatusTime,
cls.CloseDate as closeDate,
case when cls.closedate::text ~* '^[0-9]{13}$' then 
                 to_timestamp(cls.closedate::bigint/1000)
                 when cls.closedate = '0' then null
                 when cls.closedate = '' then null 
            else 
                 to_timestamp(substring(cls.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') END AS normalizedclosedate,
cls.PendingCloseDate as pendingcloseDate,
cls.PendingPropertyAddress as pendingpropertyaddress,
cls.PropertyAddress as closeAddress,
cls.PropertyPrice as closePrice,
cls.FinanceCloseDate as Financeclosedate,
cls.LenderClosedWith as lenderClosedWith,
cls.AgtCommPct as commissionPct,
cls.Concierge_Status as conciergeStatus,
cls.Concierge_Status_Time as conciergeStatusTime,
t1.created,
lrm.routingMethod as routingType,
olrm.originalroutingMethod as originalRoutingType,
CONCAT(date_part('month',ctc.enrollDate),'/1/',date_part('year',ctc.enrollDate)) as enroll_mnth_yr,
CONCAT(date_part('month',ctc.inviteDate),'/1/',date_part('year',ctc.inviteDate)) as invite_mnth_yr,
case when bac.brokerage_code is not null then '1' else '0' end as RCAssigned,
case when t2.agentBrokerageCode is null then bac.brokerage_code else t2.agentBrokerageCode end as brokerage_code,
case when t2.agentBrokerageName is null then bac.full_name else t2.agentBrokerageName end as full_name,
case when t2.aRCName is null then bac.rc_name else t2.aRCName end as RC_Name,
case when t2.agentbrokerageGroup is null then bac.RCbrokerageGroup else t2.agentbrokerageGroup end as brokerageGroup,
bac.rc_email,
bac.rc_phone,
t2.agentnetworkname,
t2.Brokerage,
t2.BrokeragePhone,
t2.BrokerageEmail,
date_part('month',t1.created),
cls.HB_Status,
date_part('dow',t1.created) as DOW,
t1.created - interval '5' hour as CST ,
date(t1.created - interval '5' hour) as CSTDate,
date_part('hour',(t1.created - interval '5' hour)) as CSTHour,
date_part('year',(t1.created - interval '5' hour)) as Year,
date_part('week',(t1.created - interval '5' hour)) as WeekNum,
case
        when date_part('hour',(ctc.enrollDate - interval '5' hour)) between 10 and 17 and date_part('dow',t1.created) between 1 and 5 then 'Business Hours'
        else 'Outside Business Hours'
        end as BusinessHours,
case when cls.HB_Status like 'Pending%' then '1'
       end as Pending,
t3.nmlsid,
t1.first_name as Client_First_Name,
t1.last_name as Client_Last_Name,
t1.price_range_lower_bound,
t1.price_range_upper_bound,
t2.AgentFirstName,
t2.AgentLastName,
case when t2.lead_id is not null then 'true' else 'false' end as agentAssigned,
bac.rc_city,
bac.rc_state,
t2.ReferralEmail,
t1.selling_address,
lts.source,
(ctc.first_contact_time - interval '5 Hours') as first_contact_time,
EXTRACT(EPOCH FROM (ctc.first_contact_time - ctc.firstAssignTime))/60/60 as timeToFirstContactHrs,
(ctc.lastContactDate - interval '5 Hours') as first_agent_contact_time,
EXTRACT(EPOCH FROM (ctc.lastContactDate - ctc.firstAssignTime))/60/60 as timeToAgentContactHrs,
(ctc.agentAcceptTime - interval '5 Hours') as agentAcceptTime,
EXTRACT(EPOCH FROM (ctc.agentAcceptTime - ctc.firstAssignTime))/60/60 as timeToAcceptHours,--ls1.RC_accept_time_unadjusted,
case when t2.lead_id is null then EXTRACT(EPOCH FROM (current_date - ctc.enrollDate))/60/60 end as waitingTimeHours,
case when lower(cls.HB_Status) like '%closed%' then 'Closed'
     when lower(cls.HB_Status) like 'active%' then 'Active'
     when lower(cls.HB_Status) like '%pending%' then 'Pending'
     when lower(cls.HB_Status) like 'inactive%' then 'Inactive'
     when lower(cls.HB_Status) like 'routing%' then 'Routing'
     when lower(cls.HB_Status) like 'invited%' then 'Invited'
     when lower(cls.HB_Status) like 'new%' then 'Concierge'
else 'Active' end as majorStatus,
ctc.leadAssignmentCount,
case when lower(LenderClosedWith) = 'chase' then 1 else 0 end as attached,
ctc.lastPendingDate,
t3.hlaSID,
t2.agentAggregateId,
t2.caeLegacyAgent,
ctc.rejectedCount,
ctc.firstAssignTime,
(ctc.lastContactDate - interval '5 Hours') as lastContactDate,
t3.totalInvitesbyLO,
total_assignments_cte.total_assignments as total_assignments,
case when rtm.rewardsTeam is not null then rtm.rewardsTeam
     else rtmn.rewardsTeamNew end as rewardsTeam,
case when rtm.claimDate is not null then (rtm.claimDate - interval '5 Hours')
     else (rtmn.claimDateNEW - interval '5 Hours') end as claimDate,
(ctc.firstAcceptedDate- interval '5 Hours') as firstAcceptedDate,
(ctc.firstConfirmedDate- interval '5 Hours') as firstConfirmedDate,
(ctc.firstRewardsTeamContactDate - interval '5 Hours')as firstRewardsTeamContact,
(ctc.firstRoutedDate- interval '5 Hours') as firstRoutedDate,
EXTRACT(EPOCH FROM (ctc.firstRoutedDate - ctc.firstAcceptedDate))/60/60 AS timeToSelectHrs,
ctc.agentAcceptCount,
ctc.agentCreatedCount,
ld.data::json->>'customRouting' as customRouting,
t1.connection_confirmed as rewardsTeamConfirmed,
t1.homestory_contacted as rewardsTeamContacted,
case when t1.connection_confirmed = 'true' then 1
     when cls.HB_Status in ('Inactive Other','Inactive Not Responsive','Active Agent Assigned','Active Attempting to Contact') then 0
     when lower(cls.HB_Status) like 'new%' then 0
     when lower(cls.HB_Status) like 'invited%' then 0
     when lower(cls.HB_Status) like 'routing%' then 0
     else 1 end as confirmedCustomerConnection,
case when ctc.EscalatedtoConcierge is null then 0
else ctc.EscalatedtoConcierge
end as EscalatedtoConcierge,
case when ZeroCoverage.ZeroCoverage is null then 0
else ZeroCoverage.ZeroCoverage
end as ZeroCoverage,
case when ctc.LowCoverage is null then 0
else ctc.LowCoverage
end as LowCoverage,
case when ctc.agentdirectTimeoutCount is null then 0
else ctc.agentdirectTimeoutCount
end as agentdirectTimeoutCount,
case when ctc.agentselectTimeoutCount is null then 0
else ctc.agentselectTimeoutCount
end as agentselectTimeoutCount,
case when ctc.RoutingAttempts is null then 0
 else ctc.RoutingAttempts
 end as RoutingAttempts,
 case when ctc.agentTimeoutCount is null then 0
else ctc.agentTimeoutCount
end as agentTimeoutCount,
case when ctc.ReassignmentCount >0 then (ctc.ReassignmentCount-1)
else 0
end as ReassignmentCount,
t1.uuid as leadUUID,
t1.aggregate_id as lead_aggregateid,
ctc.enrollDate as UnadjustedEnrollDate,
ctc.inviteDate as UnadjustedInviteDate,
ctc.firstRoutedDate as UnadjustedFirstRouteDate,
t3.HLA_first as HLA_First_Name,
t3.HLA_last as HLA_Last_Name,
case when cls.HB_Status = 'New New Referral' then '1'
        end as UnacceptedReferral,
case when t1.rc_assignable = 'true' then '1'
       end as RCDirect,
t1.id as NetworkID,
hlah.lmEmail as LMEmail,
hlah.slmEmail as SLMEmail,
t2.assignedAgentTotalReferrals,
cls.Agent_Status as AgentStatus,
cls.Agent_Status_Time as AgentStatusTime,
t4.HSAaggregateid,
t4.HSAfullname,
t2.VerificationStatus,
concat(agentHLAAssignment.hlaFirstName,' ',agentHLAAssignment.hlaLastName) as SponsorHLA,
concat(agentHLAAssignment.lmFirstName,' ',agentHLAAssignment.lmLastName) as SponsorLM,
concat(agentHLAAssignment.slmFirstName,' ',agentHLAAssignment.slmLastName) as SponsorSLM,
agentHLAAssignment.divisionName as SponsorDivision,
(ctc.lastofferedDate- interval '5 Hours') as OfferedDate,
COALESCE(ol.is_odp, false)::BOOLEAN as is_odp,
ot.ODP_agentcount,
ot.ODP_agenttype

FROM {{ ref('leads') }} t1
left outer join {{ ref('lead_traffic_sources') }} lts on lts.lead_id = t1.id
/* RC and Brokerage Details */
left outer join rc_brokerage_details bac on bac.lead_id=t1.id
/* Currently assigned Agent Details */
left outer join current_assigned_agent_details_cte t2 on t2.lead_id = t1.id
/* HLA Details */
left outer join hla_details_cte t3 on t3.lead_id = t1.id
/*HLA Assistant details*/
left outer join hla_assistant_details_cte t4 on t4.lead_id = t1.id 
/* last_agent_updates */
left join last_agent_update_cte t5 on t5.lead_id = t1.id 
/* inactive date from lead status updates */
left outer join inactive_status_update_cte inds on inds.id = t1.id
/*left outer join (select lsu.lead_id, data::json->>'invitedBy' as HLAassisstant
                from lead_status_updates lsu
                group by lsu.lead_id) t4 on t4.lead_id = t1.id*/
/* First Contact time using status = Outreach Click to Call, role = agent or RC and profile is not admin */
left outer join  lead_status_updates_cte ctc on ctc.lead_id = t1.id
/* RC, Agent, HLA assignment times. This is legacy from HS system */
left outer join ua1_cte ua1 on ua1.lead_id = t1.id
left outer join {{ ref('leads_data') }} ld on ld.lead_id = t1.id
left outer join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = t1.id
/* Last status updates by role and closing details  */
left outer join cls_cte cls on t1.id = cls.lead_id
/* Last routing method */
left outer join lrm_cte lrm on lrm.lead_id = t1.id
/* rewards team member assigned OLD Activity model */
left outer join rtm_cte rtm on t1.id = rtm.lead_id
/* rewards team member assigned NEW Assignment model */
  left outer join rtmn_cte rtmn on rtmn.lead_id = t1.id
/* Original routing method */
left outer join olrm_cte olrm on olrm.lead_id = t1.id
left outer join ZeroCoverage_cte ZeroCoverage on ZeroCoverage.lead_id = t1.id
/* HLA, including Division, SLM and LM hierarchy  */
left outer join hlah_cte hlah on hlah.hlaAggregateID = t3.hlaAggregateID and hlah.order_cte_hla = 1
left outer join agentHLAAssignment_cte agentHLAAssignment on agentHLAAssignment.agentAggregateID = t2.agentAggregateId and agentHLAAssignment.order_cte = 1
left outer join total_assignments_cte total_assignments_cte on total_assignments_cte.lead_id = t1.id
left outer join odp_leads_cte ol on t1.id = ol.lead_id and ol.row_num = 1
left outer join odp_tag_cte ot on t1.id = ot.lead_id and ot.row_num = 1
left join {{ ref('stg_lead_banks') }} slb on  t1.bank_id = slb.bank_id
WHERE  cls.HB_Status <> 'Inactive Test Referral'
)
, pre_final_cte as (
        select 
                distinct
                address,
                agent_email,
                agent_name,
                agent_phone::varchar as agent_phone,
                agentacceptcount,
                agentaccepttime,
                agentaggregateid,
                agentassigned,
                agentcommissionamount,
                agentcreatedcount,
                agentdirecttimeoutcount,
                agentfirstname,
                agentlastname,
                agentmobilephone,
                agentnetworkname,
                agentofficephone,
                agentselecttimeoutcount,
                agentstatus,
                agentstatustime,
                agenttimeoutcount,
                assignedagenttotalreferrals,
                assignee_aggregate_id,
                attached,
                brokerage,
                brokerage_code,
                brokerageemail,
                brokeragegroup,
                brokeragephone,
                businesshours,
                buytransaction,
                caelegacyagent::varchar as caelegacyagent ,
                campaign,
                cbmsa,
                chase_id,
                city,
                claimdate,
                client_contact_methods,
                client_email,
                client_first_name,
                client_last_name,
                client_name,
                client_phone,
                closeaddress,
                fec.closedate,
                fq.closedate as closedate_finance_query,
                closeprice,
                comments,
                commissionpct::varchar as commissionpct ,
                commissionpercent,
                conciergestatus,
                conciergestatustime,
                confirmedcustomerconnection,
                consumer_confirmed,
                county,
                created,
                cst,
                cstdate,
                csthour,
                customereci,
                customerfthb::varchar as customerfthb,
                customerlanguage,
                customrouting,
                date,
                date_marked_closed_final,
                date_part,
                description,
                divisionmanager,
                divisionname,
                dow,
                enroll_mnth_yr,
                enrolleddate,
                enrollmenttype,
                escalatedtoconcierge,
                financeclosedate,
                financingstatus,
                financingstatustime,
                first_agent_contact_time,
                first_contact_time,
                firstaccepteddate,
                firstassigntime,
                firstconfirmeddate,
                firstrewardsteamcontact,
                firstrouteddate,
                full_name,
                hb_status,
                hla_first_name,
                hla_last_name,
                hlaaggregateid,
                hlacheck,
                hlaentered,
                hlasid,
                hlcc_submitted,
                hsaaggregateid,
                hsafullname,
                id,
                inactivedate,
                invite_mnth_yr,
                inviteddate,
                invitemedium,
                last_agent_update,
                lastcontactdate,
                lastpendingdate,
                /*I table aliased lead_id from the finance query table since it was throwing ambiguous field errors*/
                fq.lead_id,
                leadassignmentcount,
                leaduuid,
                fec.lenderclosedwith,
                fq.lenderclosedwith as lenderclosedwith_finance_query,
                lead_aggregateid,
                lmagregateid,
                lmemail,
                lmfullname,
                lo_email,
                lo_name,
                lo_phone::varchar as lo_phone,
                loan_details_sharing_enabled,
                loenrollmentform,
                lowcoverage,
                majorstatus,
                max_date,
                networkid,
                nmlsid::varchar as nmlsid,
                normalizedcity,
                normalizedclosedate,
                normalizedstate,
                normalizedzip,
                offereddate,
                originalroutingtype,
                pending,
                pendingclosedate,
                pendingpropertyaddress,
                percentage_fee,
                prequal,
                price_range_lower_bound,
                price_range_upper_bound,
                purchase_location,
                purchase_time_frame,
                rc_city::varchar as rc_city,
                rc_email,
                rc_name,
                rc_phone,
                rc_state::varchar as rc_state,
                rcassigned,
                rcdirect,
                realestatestatus,
                realestatestatustime,
                reassignmentcount,
                rebate_paid,
                rf.referral_fee_category_id,
                referral_fee_transaction,
                referralemail,
                rejectedcount,
                reward_ready_date_final,
                reward_ready_ordered_date_final,
                rewardsteam,
                rewardsteamconfirmed,
                rewardsteamcontacted,
                routingattempts,
                routingtype,
                row_id,
                saleprice,
                selling_address,
                slmaggregateid,
                slmemail,
                slmfullname,
                smsdate,
                smspermission,
                smssource,
                source,
                sponsordivision,
                sponsorhla,
                sponsorlm,
                sponsorslm,
                st,
                stringzip,
                timetoaccepthours,
                timetoagentcontacthrs,
                timetofirstcontacthrs,
                timetoselecthrs,
                total_assignments,
                totalinvitesbylo,
                transaction_type,
                transactionstate,
                unacceptedreferral,
                unadjustedenrolldate,
                unadjustedfirstroutedate,
                unadjustedinvitedate,
                case when verificationstatus = 'Verified-Provisional' then 'Verified-Verified'
                when verificationstatus = 'Verified-Select Only' then 'Verified-Verified'
                else verificationstatus end as verificationstatus,
                verificationstatus as internal_verificationstatus,
                waitingtimehours,
                weeknum,
                year,
                zerocoverage,
                zip,
                lrf.percentage as lead_referral_fee_pct,
                rf.percentage_fee as agent_referral_fee_pct,
                fec.is_odp,
                fec.ODP_agentcount,
                fec.ODP_agenttype,
                fec.bank_id,
                fec.bank_name
        from final_enrollments_cte fec
        left join {{ ref('finance_query') }} fq 
        on fec.id = fq.lead_id 
        left join {{ ref('referral_fee') }} rf 
        on fec.agentaggregateid = rf.assignee_aggregate_id
        left join {{ ref('cbsa_locations') }} cbsa 
        on fec.normalizedZip = cbsa.zip 
        left join hlccsubmitted
        on id = hlccsubmitted.leadid        left join {{ ref('lead_referral_fee') }} lrf on lrf.lead_id = fec.id

)
SELECT  *
FROM pre_final_cte