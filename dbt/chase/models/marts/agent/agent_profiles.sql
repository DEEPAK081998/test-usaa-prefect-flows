WITH cc AS(
    select 
        profile_id, 
        count(*) as zipcount
    from 
        {{ ref('profile_coverage_zips') }} 
    group by 
        profile_id
),
ca AS(
    select profile_aggregate_id,
      count(lead_id) as referralCount
    from {{ ref('current_assignments') }} 
    where role= 'AGENT'
    group by profile_aggregate_id
),
pop_numbers_cte AS (
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
, agent_data AS(
    select
    pup.id,
    case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else pup.data->'brokerage'->>'name' end as brokerage_name,
    pup.first_name, 
    pup.last_name, 
    pup.email, 
    pup.phones, 
    pup.aggregate_id,
    pup.created, 
    pup.updated,
    cc.zipcount,
    pup.data->'brokerage'->>'brokerageCode' as brokerageCode,
    REPLACE(CAST(pup.data->'brokerage'->'email' AS TEXT), '"','') AS brokerage_email,
    REPLACE(CAST(pup.data->'brokerage'->'phones'->0->'phoneNumber' AS TEXT), '"','') AS brokerage_phone,
    pup.data->'profileInfo' ->>'stateLicences' as StateLicenses,
    pup.data->'profileInfo' ->>'agentBio' as Bio,
    pup.data->'profileInfo'->>'primaryMLSName' as MLS,
    pup.data->'profileInfo'->'totalHomesClosed' ->>'totalNumberOfHomesClosed24Months' as Units,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageHomesClosedForSellers' as sellerpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageLuxuryHomeClosings' as luxpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageHomesClosedForBuyers' as buyerpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageNewConstructionClosings' as newpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageFirstTimeHomeBuySellClosing' as fthbpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'totalHomesClosedVolume' as volume, 
    pup.data->'profileInfo'->>'trainingCompleted' as trainingcomp,
    pup.data->'profileInfo'->>'yearsWorkingInRealEstate' as experienceYears,
    json_array_length(pup.data->'languages') as languageCount,
    pup.data->'unavailableDates' as unavailableDates,
    pup.data->'reasonForRemoval' as reasonForRemoval,
    pup.data->>'productionTotalUnits' as productionTotalUnits,
    pup.data->'brokerage' as brokerage,
    pup.data->'removalReason' as removalReason,
    pup.data->'additionalNotes' as additionalNotes,
    pup.data->'nmlsid' as nmlsid,
    pup.data->'receivesReferralsViaTextMessage' as receivesReferralsViaTextMessage,
    pup.data->'coveredZips' as coveredZips,
    pup.data->'closingHours' as closingHours,
    pup.data->'verificationInputFieldsInProgress' as verificationInputFieldsInProgress,
    pup.data->'imageLink' as imageLink,
    pup.data->'stateLicenseExpiration' as stateLicenseExpiration,
    pup.data->'trainingCompleted' as trainingCompleted,
    pup.data->'address' as address,
    pup.data->>'realEstateStartDate' as realEstateStartDate,
    pup.data->'termsandConditions' as termsandConditions,
    case when pup.data->>'verificationStatus' = 'Verified-Provisional' then 'Verified-Verified' 
    when pup.data->>'verificationStatus' = 'Verified-Select Only' then 'Verified-Verified' 
    else pup.data->>'verificationStatus' end as verificationStatus,
    pup.data->>'verificationStatus' as internal_verificationstatus,
    pup.data->'cognitoUserName' as cognitoUserName,
    pup.data->'localTimezone' as localTimezone,
    pup.data->'comment' as comment,
    pup.data->'coveredAreas' as coveredAreas,
    pup.data->'networkRequirements' as networkRequirements,
    pup.data->'languages' as languages,
    pup.data->'offices' as offices,
    pup.data->'office' as office,
    pup.data->'comments' as comments,
    CAST(pup.data->'inviter'->'firstName' AS TEXT) AS inviter_firstname,
    CAST(pup.data->'inviter'->'lastName' AS TEXT) AS inviter_lastname,
    CAST(pup.data->'inviter'->'email' AS TEXT) AS inviter_email,
    CAST(pup.data->'caeLegacyAgent' AS TEXT) as caeLegacyAgent,
    pup.data->'eligibilityStatus' as eligibilityStatus,
    pup.data->'ineligibleComment' as ineligibleComment,
    pup.data->>'productionInput' as productionInput,
    pup.data->'referralAgreementEmail' as referralAgreementEmail,
    pup.data->'unverifiedProfileSections' as unverifiedProfileSections,
    pup.data->'unverfiedProfileSections' as unverfiedProfileSections,
    pup.data->'firstOnboardingStep' as firstOnboardingStep,
    pup.data->'profileInfo' as profileInfo,
    pup.data->'standardsAccepted' as standardsAccepted,
    pup.data->'reasonForDeactivation' as reasonForDeactivation,
    pup.data->'workingHours' as workingHours,
    pup.data->'acceptsReferralFees' as acceptsReferralFees,
    pup.data->'acceptsReferralsOnSundays' as acceptsReferralsOnSundays,
    pup.data->'openingHours' as openingHours,
    pup.data->'acceptsReferralsOnSaturdays' as acceptsReferralsOnSaturdays,
    ca.referralCount,
    json_array_length(pup.data->'coveredAreas') OLDzipcount,
    aop.agentOfficePhone,
    amp.agentMobilePhone
  from {{ ref('partner_user_profiles') }} pup 
  join {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
  left outer join 
    cc on cc.profile_id = pup.id
  left outer join ca on pup.aggregate_id = ca.profile_aggregate_id
  LEFT JOIN pop_office_number aop on aop.aggregate_id = pup.aggregate_id
  LEFT JOIN pop_mobile_number amp on amp.aggregate_id = pup.aggregate_id 
  where pur.role = 'AGENT'
  and pur.enabled = 'true'
),
add_legacy_flag AS(
  SELECT 
    *,
    CASE 
      WHEN caeLegacyAgent='true' THEN 'true' ELSE 'false' END as cae_legacy_agent,
    CASE 
      WHEN inviter_email LIKE '%chase%' THEN 'true' ELSE 'false' END as hla_invited_agent
  FROM 
    agent_data
)
SELECT 
concat(first_name,' ', last_name) AS agent_name,
concat(hlafirstname,' ',hlalastname) AS hla_name,
* 
FROM add_legacy_flag
LEFT JOIN {{ ref('agent_hla_hierarchy') }} hla
ON add_legacy_flag.aggregate_id=hla.agentaggregateid
