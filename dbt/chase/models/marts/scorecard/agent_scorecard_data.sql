{{ config(
    materialized = 'table',
) }}

WITH afm AS(
select pup.aggregate_id,
case when agentAcceptedCount.agentAcceptCount is null then 0 else agentAcceptedCount.agentAcceptCount end as agentAcceptCount,
case when agentRejectCounts.agentRejectCount is null then 0 else agentRejectCounts.agentRejectCount end as agentRejectCount,
case when agentCreatedCounts.agentCreatedCount is null then 0 else agentCreatedCounts.agentCreatedCount end as agentCreatedCount,
case when agentRoutedCounts.agentRoutedCount is null then 0 else agentRoutedCounts.agentRoutedCount end as agentRoutedCount,
case when agentCTCCounts.agentCTCCount is null then 0 else agentCTCCounts.agentCTCCount end as agentCTCCount,
case when agentReferralCounts.referralCount is null then 0 else agentReferralCounts.referralCount end as referralCount,
case when agentReferralCounts90.referralCount_Over90Days is null then 0 else agentReferralCounts90.referralCount_Over90Days end as referralCount_Over90Days,
case when agentTimeoutCounts.agentTimeoutCount is null then 0 else agentTimeoutCounts.agentTimeoutCount end as agentTimeoutCount,
case when agentStatusCounts.agentStatusCount is null then 0 else agentStatusCounts.agentStatusCount end as agentStatusCount,
case when agentClosedCounts.agentClosedCount is null then 0 else agentClosedCounts.agentClosedCount end as agentClosedCount,
case when Last30days_agentAcceptedCount.Last30days_agentAcceptCount is null then 0 else Last30days_agentAcceptedCount.Last30days_agentAcceptCount end as Last30days_agentAcceptCount,
case when Last30days_agentRejectCounts.Last30days_agentRejectCount is null then 0 else Last30days_agentRejectCounts.Last30days_agentRejectCount end as Last30days_agentRejectCount,
case when Last30days_agentTimeoutCounts.Last30days_agentTimeoutCount is null then 0 else Last30days_agentTimeoutCounts.Last30days_agentTimeoutCount end as Last30days_agentTimeoutCount,
case when Last30days_agentCreatedCounts.Last30days_agentCreatedCount is null then 0 else Last30days_agentCreatedCounts.Last30days_agentCreatedCount end as Last30days_agentCreatedCount,
case when Last30days_agentCTCCounts.Last30days_agentCTCCount is null then 0 else Last30days_agentCTCCounts.Last30days_agentCTCCount end as Last30days_agentCTCCount,
case when Last90days_agentAcceptedCount.Last90days_agentAcceptCount is null then 0 else Last90days_agentAcceptedCount.Last90days_agentAcceptCount end as Last90days_agentAcceptCount,
case when Last90days_agentRejectCounts.Last90days_agentRejectCount is null then 0 else Last90days_agentRejectCounts.Last90days_agentRejectCount end as Last90days_agentRejectCount,
case when Last90days_agentTimeoutCounts.Last90days_agentTimeoutCount is null then 0 else Last90days_agentTimeoutCounts.Last90days_agentTimeoutCount end as Last90days_agentTimeoutCount,
case when Last90days_agentCreatedCounts.Last90days_agentCreatedCount is null then 0 else Last90days_agentCreatedCounts.Last90days_agentCreatedCount end as Last90days_agentCreatedCount,
case when Last90days_agentCTCCounts.Last90days_agentCTCCount is null then 0 else Last90days_agentCTCCounts.Last90days_agentCTCCount end as Last90days_agentCTCCount,
case when ActiveStatus.ActiveStatus is null then 0 else ActiveStatus.ActiveStatus end as ActiveStatus,
case when PendingStatus.PendingStatus is null then 0 else PendingStatus.PendingStatus end as PendingStatus,
case when ClosedStatus.ClosedStatus is null then 0 else ClosedStatus.ClosedStatus end as ClosedStatus ,
case when InactiveStatus.InactiveStatus is null then 0 else InactiveStatus.InactiveStatus end as InactiveStatus,
case when OnholdStatus.OnholdStatus is null then 0 else OnholdStatus.OnholdStatus end as OnholdStatus,
case when OfferAcceptedStatus.OfferAcceptedStatus is null then 0 else OfferAcceptedStatus.OfferAcceptedStatus end as OfferAcceptedStatus,
case when ListingApptStatus.ListingApptStatus is null then 0 else ListingApptStatus.ListingApptStatus end as ListingApptStatus
from {{ ref('partner_user_profiles') }} pup
    JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
/* twilio agent accepted count */
    left outer join (select profile_aggregate_id, count(lead_id) as agentAcceptCount, max(created) as lastReferralDate
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.accepted'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentAcceptedCount on agentAcceptedCount.profile_aggregate_id = pup.aggregate_id
/*twilio timeout count*/
     left outer join (select profile_aggregate_id, count (lead_id) as agentTimeoutCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.timeout'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentTimeoutCounts on agentTimeoutCounts.profile_aggregate_id = pup.aggregate_id
/* twilio agent rejected count */
    left outer join (select profile_aggregate_id, count (lead_id) as agentRejectCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.rejected'
                    AND lower(role) in ('agent','referral_coordinator')
                    group by profile_aggregate_id) agentRejectCounts on agentRejectCounts.profile_aggregate_id = pup.aggregate_id
/*twilio created count*/
     left outer join (select profile_aggregate_id, count (distinct lead_id) as agentCreatedCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.created'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentCreatedCounts on agentCreatedCounts.profile_aggregate_id = pup.aggregate_id
/*twilio routed count*/
     left outer join (select profile_aggregate_id, count (lead_id) as agentRoutedCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.created'
                    AND lower(role) in ('agent','referral_coordinator') 
                    group by profile_aggregate_id) agentRoutedCounts on agentRoutedCounts.profile_aggregate_id = pup.aggregate_id
/* twilio CTC completed*/
 left outer join (select profile_aggregate_id, count (lead_id) as agentCTCCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE status LIKE 'Outreach Click to Call'
                    AND lower(role) = 'agent'
                    group by profile_aggregate_id) agentCTCCounts on agentCTCCounts.profile_aggregate_id = pup.aggregate_id
left outer join (select profile_aggregate_id, count(lead_id) as currentAssignments
                    from {{ ref('current_assignments') }}  
                    where role = 'AGENT'
                    group by profile_aggregate_id) c on c.profile_aggregate_id = pup.aggregate_id
--Activity Counts for the last 30 days only
/* twilio agent accepted count */
    left outer join (select profile_aggregate_id, count(*) as Last30days_agentAcceptCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.accepted'
                    AND lower(role) ='agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentAcceptedCount on Last30days_agentAcceptedCount.profile_aggregate_id = pup.aggregate_id
/*twilio timeout count*/
     left outer join (select profile_aggregate_id, count (*) as Last30days_agentTimeoutCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.timeout'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentTimeoutCounts on Last30days_agentTimeoutCounts.profile_aggregate_id = pup.aggregate_id
/* twilio agent rejected count */
    left outer join (select profile_aggregate_id, count (*) as Last30days_agentRejectCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.rejected'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentRejectCounts on Last30days_agentRejectCounts.profile_aggregate_id = pup.aggregate_id
/*twilio created count*/
     left outer join (select profile_aggregate_id, count (*) as Last30days_agentCreatedCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.created'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentCreatedCounts on Last30days_agentCreatedCounts.profile_aggregate_id = pup.aggregate_id
/* twilio CTC completed*/
 left outer join (select profile_aggregate_id, count (*) as Last30days_agentCTCCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE status LIKE 'Outreach Click to Call'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 31
                    group by profile_aggregate_id) Last30days_agentCTCCounts on Last30days_agentCTCCounts.profile_aggregate_id = pup.aggregate_id
--end 30 days status counts

--Activity Counts for the last 90 days only
/* twilio agent accepted count */
    left outer join (select profile_aggregate_id, count(*) as Last90days_agentAcceptCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.accepted'
                    AND lower(role) ='agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentAcceptedCount on Last90days_agentAcceptedCount.profile_aggregate_id = pup.aggregate_id
/*twilio timeout count*/
     left outer join (select profile_aggregate_id, count (*) as Last90days_agentTimeoutCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.timeout'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentTimeoutCounts on Last90days_agentTimeoutCounts.profile_aggregate_id = pup.aggregate_id
/* twilio agent rejected count */
    left outer join (select profile_aggregate_id, count (*) as Last90days_agentRejectCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.rejected'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentRejectCounts on Last90days_agentRejectCounts.profile_aggregate_id = pup.aggregate_id
/*twilio created count*/
     left outer join (select profile_aggregate_id, count (*) as Last90days_agentCreatedCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE data->>'twilioEvent' = 'reservation.created'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentCreatedCounts on Last90days_agentCreatedCounts.profile_aggregate_id = pup.aggregate_id
/* twilio CTC completed*/
 left outer join (select profile_aggregate_id, count (*) as Last90days_agentCTCCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE status LIKE 'Outreach Click to Call'
                    AND lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) < 91
                    group by profile_aggregate_id) Last90days_agentCTCCounts on Last90days_agentCTCCounts.profile_aggregate_id = pup.aggregate_id
--end 90 days status counts
/*Status Updates */
left outer join (select profile_aggregate_id, count (*) as agentStatusCount
                    from {{ ref('lead_status_updates') }} 
                    WHERE category like 'Property%'
                    and status not in ('New New Referral', 'Active Agent Assigned') 
                    and status not like ('Routing%')
                    and status not like ('Update%')
                    AND lower(role) = 'agent'
                    group by profile_aggregate_id) agentStatusCounts on agentstatusCounts.profile_aggregate_id = pup.aggregate_id
/*Referral Counts*/
left outer join(select profile_aggregate_id, count(lead_id) as referralCount
                    from {{ ref('current_assignments') }} 
                    where lower(role) = 'agent'
                    group by profile_aggregate_id) agentReferralCounts on agentReferralCounts.profile_aggregate_id = pup.aggregate_id
/*Referral Counts - Over 90 Days*/
left outer join(select profile_aggregate_id, count(lead_id) as referralCount_Over90Days
                    from {{ ref('current_assignments') }}
                    where lower(role) = 'agent'
                    and DATE_PART('day', current_date - created) >= 90
                    group by profile_aggregate_id) agentReferralCounts90 on agentReferralCounts90.profile_aggregate_id = pup.aggregate_id
/*Closed Count*/
left outer join (select ca.profile_aggregate_id, count (cls.lead_id) as agentClosedCount
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Closed%'
                    group by ca.profile_aggregate_id) agentClosedCounts on agentClosedCounts.profile_aggregate_id = pup.aggregate_id
/*Current Active Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as ActiveStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Active%'
                    group by ca.profile_aggregate_id) ActiveStatus on ActiveStatus.profile_aggregate_id = pup.aggregate_id
/*Current Pending Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as PendingStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Pending%'
                    group by ca.profile_aggregate_id) PendingStatus on PendingStatus.profile_aggregate_id = pup.aggregate_id
/*Current Closed Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as ClosedStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Closed%'
                    group by ca.profile_aggregate_id) ClosedStatus on ClosedStatus.profile_aggregate_id = pup.aggregate_id
/*Current Inactive Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as InactiveStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Inactive%'
                    group by ca.profile_aggregate_id) InactiveStatus on InactiveStatus.profile_aggregate_id = pup.aggregate_id
/*Current On Hold Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as OnholdStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'On Hold%'
                    group by ca.profile_aggregate_id) OnholdStatus on Onholdstatus.profile_aggregate_id = pup.aggregate_id
/*Current Offer Accepted Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as OfferAcceptedStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Offer Accepted%'
                    group by ca.profile_aggregate_id) OfferAcceptedStatus on OfferAcceptedStatus.profile_aggregate_id = pup.aggregate_id
/*Current Listing Appointment Status*/
left outer join (select ca.profile_aggregate_id, count(cls.lead_id) as ListingApptStatus
                    from {{ ref('current_lead_statuses') }} cls
                    join {{ ref('current_assignments') }} ca on ca.lead_id = cls.lead_id
                    where category like 'Property%'
                    and status like 'Listing Appointment%%'
                    group by ca.profile_aggregate_id) ListingApptStatus on ListingApptStatus.profile_aggregate_id = pup.aggregate_id
)
SELECT 
    acceptsreferralfees,
    acceptsreferralsonsaturdays,
    acceptsreferralsonsundays,
    activestatus,
    additionalnotes,
    address,
    agent_name,
    agentacceptcount,
    agentaggregateid,
    agentclosedcount,
    agentcreatedcount,
    agentctccount,
    agentfirstname,
    agentid,
    agentlastname,
    agentrejectcount,
    agentrole,
    agentroutedcount,
    agentstatuscount,
    agenttimeoutcount,
    afm.aggregate_id,
    bio,
    brokerage,
    brokerage_email,
    brokerage_name,
    brokerage_phone,
    brokeragecode,
    buyerpct,
    cae_legacy_agent,
    caelegacyagent,
    closedstatus,
    closinghours,
    cognitousername,
    comment,
    comments,
    coveredareas,
    coveredzips,
    created,
    divisionmanager,
    divisionname,
    eligibilitystatus,
    email,
    experienceyears,
    first_name,
    firstonboardingstep,
    fthbpct,
    hla_invited_agent,
    hla_name,
    hlaagregateid,
    hlaemail,
    hlafirstname,
    hlaid,
    hlaidcheck,
    hlalastname,
    hlanmlsid,
    hlaphone,
    hlarole,
    id,
    imagelink,
    inactivestatus,
    ineligiblecomment,
    inviter_email,
    inviter_firstname,
    inviter_lastname,
    languagecount,
    languages,
    last30days_agentacceptcount,
    last30days_agentcreatedcount,
    last30days_agentctccount,
    last30days_agentrejectcount,
    last30days_agenttimeoutcount,
    last90days_agentacceptcount,
    last90days_agentcreatedcount,
    last90days_agentctccount,
    last90days_agentrejectcount,
    last90days_agenttimeoutcount,
    last_name,
    listingapptstatus,
    lmemail,
    lmfirstname,
    lmid,
    lmidcheck,
    lmlastname,
    localtimezone,
    luxpct,
    mls,
    networkrequirements,
    newpct,
    nmlsid,
    offeracceptedstatus,
    office,
    offices,
    oldzipcount,
    onholdstatus,
    openinghours,
    pendingstatus,
    phones,
    productioninput,
    productiontotalunits,
    profileinfo,
    realestatestartdate,
    reasonfordeactivation,
    reasonforremoval,
    receivesreferralsviatextmessage,
    referralagreementemail,
    afm.referralcount,
    referralcount_over90days,
    removalreason,
    sellerpct,
    slmemail,
    slmfirstname,
    slmid,
    slmlastname,
    standardsaccepted,
    statelicenseexpiration,
    statelicenses,
    termsandconditions,
    trainingcomp,
    trainingcompleted,
    unavailabledates,
    units,
    unverfiedprofilesections,
    unverifiedprofilesections,
    updated,
    verificationinputfieldsinprogress,
    case when verificationstatus = 'Verified-Provisional' then 'Verified-Verified'
    when verificationstatus = 'Verified-Select Only' then 'Verified-Verified'
    else verificationstatus end as verificationstatus,
    verificationstatus as internal_verificationstatus,
    volume,
    workinghours,
    zipcount

FROM afm
LEFT JOIN agent_profiles ap
ON afm.aggregate_id=ap.aggregate_id