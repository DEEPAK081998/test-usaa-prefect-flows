{{ config(
    materialized = 'table',
) }}
WITH agentcoverage AS (
    select 
        pup.aggregate_id, 
        pc.profile_id, 
        pc.zip,
        cbsa.city, 
        cbsa.county, 
        cbsa.st, 
        cbsa.cbmsa
    from {{ ref('profile_coverage_zips') }}  pc
    join {{ ref('partner_user_profiles') }}  pup on pc.profile_id = pup.id
    join {{ ref('cbsa_locations') }} cbsa on pc.zip = cbsa.zip
),
agentscores AS(
with scores_cte as
(
with rates_cte as
(
select aggregate_id, agentacceptcount,agentoffercount,
case when currentreferrals_over90days > 0 then (closedreferrals_over90days::decimal/currentreferrals_over90days::decimal)
else 0 end as closePct,
case when Last90Days_agentOffercount > 0 then ((Last90Days_agentAcceptcount::decimal+Last90Days_agentRejectCount::decimal)/Last90Days_agentOfferCount::decimal) end as L90_responsePct,
case when Last90Days_agentOffercount > 0 then (Last90days_updates_in_compliance::decimal/Last90Days_agentOfferCount::decimal) end as L90_updatePCT
from
fct_agent_scorecard
)
select aggregate_id,closePct, l90_responsepct, L90_updatePCT,
case when agentacceptcount<=3 then 3
     when closePct >= .15 then 5
     when closePct >=.1 then 4
     when closePct >=.09 then 3
     when closePct >=.08 then 2
     when closePct <.08 then 1
else 3 end as CloseScore,
case when agentoffercount = 0 then 3
    when l90_responsepct>=.80 then 5
    when l90_responsepct>=.75 then 4
    when l90_responsepct>=.5 then 3
    when l90_responsepct>=.3 then 2
    when l90_responsepct <.3 then 1
else 3 end as ResponseScore,
case when agentacceptcount=0 then 3
     when L90_updatePCT >=.9 then 5     
     when L90_updatePCT >=.80 then 4
     when L90_updatePCT >=.75 then 3
     when L90_updatePCT >=.5 then 2
     when L90_updatePCT <.5 then 1
else 3 end as UpdateScore
from rates_cte
)
select aggregate_id, closePct, L90_responsePct, L90_updatePct,
CloseScore, ResponseScore, UpdateScore, (coalesce(CloseScore,3)+ coalesce(ResponseScore,3)+coalesce(UpdateScore,3))/3 as agentScore
 from scores_cte)
SELECT 
    acceptpct,
    acceptsreferralfees,
    acceptsreferralsonsaturdays,
    acceptsreferralsonsundays,
    activereferrals,
    additionalnotes,
    address,
    agent_name,
    agentacceptcount,
    agentaggregateid,
    agentctccount,
    agentfirstname,
    agentid,
    agentlastname,
    agentoffercount,
    agentrejectcount,
    agentrole,
    agentscore,
    agentstatuscount,
    agenttimeoutcount,
    fas.aggregate_id,
    assignee_aggregate_id,
    bio,
    brokerage,
    brokerage_email,
    brokerage_name,
    brokerage_phone,
    brokeragecode,
    buyerpct,
    cae_legacy_agent,
    caelegacyagent,
    cbmsa,
    city,
    closedreferrals,
    closedreferrals_over90days,
    fas.closepct,
    closescore,
    closinghours,
    cognitousername,
    comment,
    comments,
    county,
    coveredareas,
    coveredzips,
    created,
    currentreferrals,
    currentreferrals_acceptedlast30days,
    currentreferrals_acceptedlast90days,
    currentreferrals_over90days,
    description,
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
    inactivereferrals,
    ineligiblecomment,
    inviter_email,
    inviter_firstname,
    inviter_lastname,
    l30_acceptpct,
    l30_responsepct,
    l30_timeoutpct,
    l90_acceptpct,
    fas.l90_responsepct,
    l90_timeoutpct,
    l90_updatepct,
    languagecount,
    languages,
    last30days_agentacceptcount,
    last30days_agentctccount,
    last30days_agentoffercount,
    last30days_agentrejectcount,
    last30days_agentstatuscount,
    last30days_agenttimeoutcount,
    last30days_updates_in_compliance,
    last90days_agentacceptcount,
    last90days_agentctccount,
    last90days_agentoffercount,
    last90days_agentrejectcount,
    last90days_agentstatuscount,
    last90days_agenttimeoutcount,
    last90days_updates_in_compliance,
    last_name,
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
    office,
    offices,
    oldzipcount,
    onholdreferrals,
    openinghours,
    pendingreferrals,
    percentage_fee,
    phones,
    productioninput,
    productiontotalunits,
    profile_id,
    profileinfo,
    program_name,
    realestatestartdate,
    reasonfordeactivation,
    reasonforremoval,
    receivesreferralsviatextmessage,
    referral_fee_category_id,
    referralagreementemail,
    referralcount,
    removalreason,
    responsepct,
    responsescore,
    sellerpct,
    slmemail,
    slmfirstname,
    slmid,
    slmlastname,
    st,
    standardsaccepted,
    statelicenseexpiration,
    statelicenses,
    termsandconditions,
    timeoutpct,
    trainingcomp,
    trainingcompleted,
    unavailabledates,
    units,
    unverfiedprofilesections,
    unverifiedprofilesections,
    updated,
    updates_in_compliance,
    updatescore,
    verification_status,
    verificationinputfieldsinprogress,
    verificationstatus,
    volume,
    workinghours,
    zip,
    zipcount
FROM
    {{ ref('fct_agent_scorecard') }} fas
LEFT JOIN {{ ref('agent_profiles') }} ap
ON fas.aggregate_id=ap.aggregate_id
LEFT JOIN {{ ref('referral_fee') }} rf
ON fas.aggregate_id=rf.assignee_aggregate_id
LEFT JOIN agentcoverage ac
ON fas.aggregate_id=ac.aggregate_id
LEFT JOIN agentscores ags 
ON fas.aggregate_id=ags.aggregate_id
