{{
  config(
	materialized = 'table'
	)
}}
WITH
lead_status_cte  AS (
    select
        profile_aggregate_id,
        count(case when data->>'twilioEvent' = 'reservation.accepted' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentAcceptCount,
        count(case when data->>'twilioEvent' = 'reservation.timeout' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentTimeoutCount,
        count(case when data->>'twilioEvent' = 'reservation.rejected' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentRejectCount,
        count(distinct case when data->>'twilioEvent' = 'reservation.created' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentCreatedCount,
        count(case when data->>'twilioEvent' = 'reservation.created' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.lead_id END) as agentRoutedCount,
        count(case when status LIKE 'Outreach Click to Call' AND lower(role) = 'agent' THEN  lsu.lead_id END) as agentCTCCount,
        count(case when category like 'Property%' and status not in ('New New Referral', 'Active Agent Assigned') and status not like ('Routing%') and status not like ('Update%') AND lower(role) = 'agent' THEN  lsu.lead_id END) as agentStatusCount,
        max(case when data->>'twilioEvent' = 'reservation.accepted' AND lower(role) in ('agent','referral_coordinator') THEN  lsu.created END) as lastReferralDate
    from {{ ref('lead_status_updates') }} lsu
    group by 
        profile_aggregate_id
)
, current_assignments_cte AS (
    select profile_aggregate_id, count(ca.lead_id) as referralCount
    from {{ ref('current_assignments') }} ca
    join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = ca.lead_id
    where lower(role) = 'agent'
    group by profile_aggregate_id
)
, current_lead_statuses_cte AS (
    select 
        ca.profile_aggregate_id,
        count( case when category like 'Property%' and status like 'Closed%' then cls.lead_id end) as agentClosedCount,
        count( case when category like 'Property%' and status like 'Pending%' then cls.lead_id end) as PendingStatus,
        count( case when category like 'Property%' and status like 'Inactive%' then cls.lead_id end) as InactiveStatus,
        count( case when category like 'Property%' and status like 'On Hold%' then cls.lead_id end) as OnholdStatus,
        count( case when category like 'Property%' and status like 'Offer Accepted%' then cls.lead_id end) as OfferAcceptedStatus,
        count( case when category like 'Property%' and status like 'Active%' then cls.lead_id end) as ActiveStatus
    from {{ ref('current_lead_statuses') }} cls
    join {{ ref('current_assignments') }}  ca on ca.lead_id = cls.lead_id
    group by 
        ca.profile_aggregate_id
)
,afm AS (
    select
    pup.aggregate_id,
    pup.id,
    lsc.agentCreatedCount,
    lsc.agentRoutedCount,
    lsc.agentAcceptCount,
    lsc.agentRejectCount,
    lsc.agentTimeoutCount,
    lsc.agentCTCCount,
    lsc.agentStatusCount,
    lsc.lastReferralDate,
    cac.referralCount,
    clsc.agentClosedCount,
    clsc.PendingStatus,
    clsc.OfferAcceptedStatus,
    clsc.ActiveStatus,
    clsc.OnholdStatus,
    clsc.InactiveStatus
from {{ ref('partner_user_profiles') }} pup
join lead_status_cte lsc on lsc.profile_aggregate_id = pup.aggregate_id
join current_assignments_cte cac on cac.profile_aggregate_id = pup.aggregate_id
join current_lead_statuses_cte clsc on clsc.profile_aggregate_id = pup.aggregate_id
)
select * from afm