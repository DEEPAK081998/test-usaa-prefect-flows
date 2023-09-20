{{ config(
    materialized = 'table',
    enabled=true
) }}

WITH 

pop_cte AS (
    SELECT
        aggregate_id,
        json_array_elements(phones::json)->>'phoneType' AS phonetype,
        json_array_elements(phones::json)->>'phoneNumber' AS phoneNumber
    FROM {{ ref('partner_user_profiles') }}
)

, amp_cte AS (
    SELECT
        aggregate_id,
        MIN(phonenumber) AS agentMobilePhone
    FROM pop_cte
    WHERE LOWER(phonetype) = 'mobilephone'
    GROUP BY aggregate_id
)

, aop_cte AS (
    SELECT
        aggregate_id,
        MIN(phonenumber) AS agentOfficePhone
    FROM pop_cte
    WHERE LOWER(phonetype) = 'office'
    GROUP BY aggregate_id
)

, agentStatusCount_cte AS (
    SELECT
        lsu.lead_id,
        count(case when category like 'Property%' and status not in ('New New Referral', 'Active Agent Assigned') and status not like ('Routing%') and status not like ('Update%') AND lower(role) = 'agent' THEN  lsu.lead_id END) as agentStatusCount
    FROM {{ ref('lead_status_updates') }} lsu
    GROUP BY lsu.lead_id
)
, HB_Status_inner_cte AS (
  SELECT lead_id, max(id) as id
  FROM {{ ref('current_lead_statuses') }}
  WHERE category in ('PropertySell','PropertySearch')
  GROUP BY lead_id
)
, HB_status_cte AS (
    SELECT 
      cs.lead_id, cs.status as HB_Status, (cs.created-interval '5 hour') as HB_Status_Time
    FROM {{ ref('current_lead_statuses') }} cs
    join HB_Status_inner_cte mid on cs.id = mid.id
)
, enrolldate_cte AS (
    SELECT
      lead_id, min(created) as enrollDate
    FROM {{ ref('lead_status_updates') }}
    WHERE category in ('PropertySell','PropertySearch') and lower(status) not like ('invite%')
    GROUP BY lead_id
)
, assignedAgentTotalReferrals_cte AS (
  SELECT profile_aggregate_id, count(distinct lead_id) as assignedAgentTotalReferrals
  FROM {{ ref('lead_status_updates') }}
  WHERE lower (role) = 'agent'
  GROUP BY profile_aggregate_id
)
, t2_cte AS (
  SELECT ca.lead_id, ca.role, pup.email as email,
                        pup.phones as phone,
                        aop.agentOfficePhone,
                        amp.agentMobilePhone,
                        concat(pup.first_name,' ',pup.last_name) as fullName,
                        pup.first_name as AgentFirstName,
                        pup.last_name as AgentLastName,
                        pup.brokerage_code as agentBrokerageCode,
                        pup.verification_status as VerificationStatus,
                        b.full_name as agentBrokerageName,
                        pup.aggregate_id as agentAggregateId,
                        pup.id as agentID,
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
                        from {{ ref('current_assignments') }} ca
                        join {{ ref('partner_user_profiles') }} pup on ca.profile_aggregate_id = pup.aggregate_id
                        left join {{ ref('brokerages') }} b on pup.brokerage_code = b.brokerage_code
                        left join {{ ref('partner_user_relationships') }} pur on b.aggregate_id = pur.parent_profile_id
                        /*left join brokerage_user bu on bu.brokerage_code = pup.brokerage_code*/
                        left join {{ ref('partner_user_profiles') }} pup2 on pup2.aggregate_id = pur.child_profile_id
                        left outer join aop_cte aop on aop.aggregate_id = ca.profile_aggregate_id
                        left outer join amp_cte amp on amp.aggregate_id = ca.profile_aggregate_id
                        left outer join assignedAgentTotalReferrals_cte atr on atr.profile_aggregate_id = ca.profile_aggregate_id
                     where ca.role='AGENT'
)
select lsu.lead_id,
ed.enrollDate,
lsu.data::json->'twilioEvent' as twilio_event,
lsu.profile_aggregate_id,
lsu.status,
concat(pup.first_name,' ',pup.last_name) as agent_name,
concat(t2.AgentFirstName,' ',t2.AgentLastName) as CA_agent_name,
t2.agentAggregateId as CA_aggregate_id,
case when lsu.profile_aggregate_id = t2.agentAggregateId then 1 else 0 end as currently_assigned,
pup.brokerage_code,
case when pup.brokerage_code is null then pup.data::json->'brokerage'->>'fullName' else b.full_name end as Brokeragename,
pup.email,
pup.verification_status,
amp.agentMobilePhone,
aop.agentOfficePhone,
asct.agentStatusCount,
/*asctt.ascount,*/
case when lower(hb.HB_Status) like '%closed%' then 'Closed'
     when lower(hb.HB_Status) like 'active%' then 'Active'
     when lower(hb.HB_Status) like '%pending%' then 'Pending'
     when lower(hb.HB_Status) like 'inactive%' then 'Inactive'
     when lower(hb.HB_Status) like 'routing%' then 'Routing'
     when lower(hb.HB_Status) like 'invited%' then 'Invited'
     when lower(hb.HB_Status) like 'new%' then 'Concierge'
else 'Active' end as majorStatus,
case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'zip' else nl.normalized_purchase_location::json->>'zip' end as normalizedZip
from {{ ref('lead_status_updates') }} lsu
join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = lsu.profile_aggregate_id
join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = lsu.lead_id
join {{ ref('leads') }} t1 on t1.id = lsu.lead_id 
left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
left outer join enrolldate_cte ed on ed.lead_id = lsu.lead_id
left outer join t2_cte t2 on t2.lead_id = lsu.lead_id
left outer join HB_status_cte hb on hb.lead_id = lsu.lead_id
left outer join agentStatusCount_cte asct on asct.lead_id = lsu.lead_id
left outer join amp_cte amp on amp.aggregate_id = lsu.profile_aggregate_id
left outer join aop_cte aop on aop.aggregate_id = lsu.profile_aggregate_id
where lsu.role = 'AGENT'