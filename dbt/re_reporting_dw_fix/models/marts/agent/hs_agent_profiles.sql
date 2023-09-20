WITH agent_data_cte AS(
  select

    {% if target.type == 'snowflake' %}
  
    pup.id,
    pup.data:brokerage.fullName::VARCHAR as brokerage_name,
    /*case when pup.brokerage_code is null then pup.data:brokerage.fullName::VARCHAR else pup.data:brokerage.fullName::VARCHAR end as brokerage_name,*/
    pup.first_name,
    pup.last_name,
    pup.email,
    pup.phones,
    pup.aggregate_id,
    pup.created,
    pup.updated,
    cc.zipcount,
    pup.data:brokerage.brokerageCode::VARCHAR as brokerageCode,
    REPLACE(CAST(pup.data:brokerage.email AS TEXT), '"','') AS brokerage_email,
    REPLACE(CAST(pup.data:brokerage.phones[0].phoneNumber AS TEXT), '"','') AS brokerage_phone,
    pup.data:profileInfo.stateLicences::VARCHAR as StateLicenses,
    pup.data:profileInfo.agentBio::VARCHAR as Bio,
    pup.data:profileInfo.primaryMLSName::VARCHAR as MLS,
    pup.data:profileInfo.totalHomesClosed.totalNumberOfHomesClosed24Months::VARCHAR as Units,
    pup.data:profileInfo.totalHomesClosed.percentageHomesClosedForSellers::VARCHAR as sellerpct,
    pup.data:profileInfo.totalHomesClosed.percentageLuxuryHomeClosings::VARCHAR as luxpct,
    pup.data:profileInfo.totalHomesClosed.percentageHomesClosedForBuyers::VARCHAR as buyerpct,
    pup.data:profileInfo.totalHomesClosed.percentageNewConstructionClosings::VARCHAR as newpct,
    pup.data:profileInfo.totalHomesClosed.percentageFirstTimeHomeBuySellClosing::VARCHAR as fthbpct,
    pup.data:profileInfo.totalHomesClosed.totalHomesClosedVolume::VARCHAR as volume,
    pup.data:profileInfo.trainingCompleted::VARCHAR as trainingcomp,
    pup.data:profileInfo.yearsWorkingInRealEstate::VARCHAR as experienceYears,
    ARRAY_SIZE(pup.data:languages) as languageCount,
    pup.data:unavailableDates as unavailableDates,
    pup.data:reasonForRemoval as reasonForRemoval,
    pup.data:productionTotalUnits::VARCHAR as productionTotalUnits,
    pup.data:brokerage as brokerage,
    pup.data:removalReason as removalReason,
    pup.data:additionalNotes as additionalNotes,
    pup.data:nmlsid as nmlsid,
    pup.data:receivesReferralsViaTextMessage as receivesReferralsViaTextMessage,
    pup.data:coveredZips as coveredZips,
    pup.data:closingHour as closingHours,
    pup.data:verificationInputFieldsInProgress as verificationInputFieldsInProgress,
    pup.data:imageLink as imageLink,
    pup.data:stateLicenseExpiration as stateLicenseExpiration,
    pup.data:trainingCompleted as trainingCompleted,
    pup.data:address as address,
    pup.data:realEstateStartDate::VARCHAR as realEstateStartDate,
    pup.data:termsandConditions as termsandConditions,
    pup.data:verificationStatus::VARCHAR as verificationStatus,
    pup.data:cognitoUserName as cognitoUserName,
    pup.data:localTimezone as localTimezone,
    pup.data:comment as comment,
    pup.data:coveredAreas as coveredAreas,
    pup.data:networkRequirements as networkRequirements,
    pup.data:languages as languages,
    pup.data:offices as offices,
    pup.data:office as office,
    pup.data:comments as comments,
    CAST(pup.data:inviter.firstName AS TEXT) AS inviter_firstname,
    CAST(pup.data:inviter.lastName AS TEXT) AS inviter_lastname,
    CAST(pup.data:inviter.email AS TEXT) AS inviter_email,
    CAST(pup.data:caeLegacyAgent AS TEXT) as caeLegacyAgent,
    pup.data:eligibilityStatus as eligibilityStatus,
    pup.data:ineligibleComment as ineligibleComment,
    pup.data:productionInput::VARCHAR as productionInput,
    pup.data:referralAgreementEmail as referralAgreementEmail,
    pup.data:unverifiedProfileSections as unverifiedProfileSections,
    pup.data:unverfiedProfileSections as unverfiedProfileSections,
    pup.data:firstOnboardingStep as firstOnboardingStep,
    pup.data:profileInfo as profileInfo,
    pup.data:standardsAccepted as standardsAccepted,
    pup.data:reasonForDeactivation as reasonForDeactivation,
    pup.data:workingHours as workingHours,
    pup.data:acceptsReferralFees as acceptsReferralFees,
    pup.data:acceptsReferralsOnSundays as acceptsReferralsOnSundays,
    pup.data:openingHours as openingHours,
    pup.data:acceptsReferralsOnSaturdays as acceptsReferralsOnSaturdays,
    ca.referralCount,
    ARRAY_SIZE(pup.data:coveredAreas) OLDzipcount,
    case when
      lower(pup.data:verificationStatus::VARCHAR) like 'verified%pending%broker%approval%' then 1
      else 0
      end as mra_sent,
    case when
			(pup.data:brokerage.data.mraHistoryData) is not null then 1 else 0
			end as mra_received,
    pup.data:brokerage.data.mraHistoryData as mra_data,
    pup.data:brokerage.data.agreementHistory as tnc_data

    {% else %}

    pup.id,
    pup.data->'brokerage'->>'fullName' as brokerage_name,
    /*case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else pup.data->'brokerage'->>'fullName' end as brokerage_name,*/
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
    pup.data->>'verificationStatus' as verificationStatus,
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
    case when
      lower(pup.data->>'verificationStatus') like 'verified%pending%broker%approval%' then 1
      else 0
      end as mra_sent,
    case when
			(pup.data -> 'brokerage' -> 'data' -> 'mraHistoryData') is not null then 1 else 0
			end as mra_received,
    pup.data -> 'brokerage' -> 'data' -> 'mraHistoryData' as mra_data,
    pup.data -> 'agreementHistory' as tnc_data

  {% endif %} 

  from 
    {{ ref('partner_user_profiles') }} pup 
    join {{ ref('partner_user_roles') }}  pur 
      on pup.id = pur.user_profile_id
    left outer join (
      select 
        profile_id, 
        count(*) as zipcount
      from 
        {{ ref('profile_coverage_zips') }}
      group by 
        profile_id) cc 
      on cc.profile_id = pup.id
  left outer join (
      select 
        profile_aggregate_id,
        count(lead_id) as referralCount
      from 
        {{ ref('current_assignments') }}
      where 
        role = 'AGENT'
      group by 
        profile_aggregate_id) ca 
      on pup.aggregate_id = ca.profile_aggregate_id
  where pur.role = 'AGENT'
),
mra_data_split_cte as (
  {% if target.type == 'snowflake' %}

  select 
      t.aggregate_id,
      f.value as mra_data
  from 
      agent_data_cte t, lateral flatten (input => t.mra_data) f

  {% else %}

	select 
		aggregate_id,
		json_array_elements(mra_data) as mra_data
	from 
		agent_data_cte	
    
  {% endif %}
),
mra_data_normalized_cte as (
	select 
		aggregate_id,
		mra_data,

    {% if target.type == 'snowflake' %}

		mra_data:mraSigned as mra_signed,
		to_timestamp_ltz((mra_data:mraSignedDate)::numeric/1000)::date as mra_signed_date,
		mra_data:referralAgreementLink as mra_link,
		mra_data:comment as mra_comment,
		to_timestamp_ltz((mra_data:timeStamp)::numeric/1000) as mra_timestamp,
		mra_data:user.id as mra_user_id,
		mra_data:user.fullName as mra_user_fullname,
		mra_data:user.email as mra_user_email,
		mra_data:source as mra_source,
		row_number() over
			(partition by aggregate_id order by mra_data:mraSignedDate desc, mra_data:timeStamp desc) as row_id

    {% else %}
    
		mra_data ->> 'mraSigned' as mra_signed,
		to_timestamp((mra_data ->> 'mraSignedDate')::numeric/1000)::date as mra_signed_date,
		mra_data ->> 'referralAgreementLink' as mra_link,
		mra_data ->> 'comment' as mra_comment,
		to_timestamp((mra_data ->> 'timeStamp')::numeric/1000)::timestamptz as mra_timestamp,
		mra_data -> 'user' ->> 'id' as mra_user_id,
		mra_data -> 'user' ->> 'fullName' as mra_user_fullname,
		mra_data -> 'user' ->> 'email' as mra_user_email,
		mra_data ->> 'source' as mra_source,
		row_number() over
			(partition by aggregate_id order by mra_data ->> 'mraSignedDate' desc, mra_data ->> 'timeStamp' desc) as row_id
    
    {% endif %}

	from 
		mra_data_split_cte	
),
mra_data_cte as (
	select 
		aggregate_id,
		mra_signed,
		mra_signed_date,
		mra_link,
		mra_comment,
		mra_timestamp,
		mra_user_id,
		mra_user_fullname,
		mra_user_email,
		mra_source
	from 
		mra_data_normalized_cte	
	where
		row_id = 1
),
tnc_data_split_cte as (
  {% if target.type == 'snowflake' %}

  select 
      t.aggregate_id,
      f.value as tnc_data
  from 
      agent_data_cte t, lateral flatten (input => t.tnc_data) f

  {% else %}

	select 
		aggregate_id, 
		json_array_elements(tnc_data) as tnc_data
	from 
		agent_data_cte		
    
  {% endif %}	
),
tnc_data_normalized as (
	select
		aggregate_id,

    {% if target.type == 'snowflake' %}

    (tnc_data:agreementDate)::timestamptz as agreement_date,
		tnc_data:agreementVersion as agreement_version,
		row_number() over
			(partition by aggregate_id order by (tnc_data:agreementDate)::timestamptz desc) as row_id

    {% else %}
      
    (tnc_data ->> 'agreementDate')::timestamptz as agreement_date,
		tnc_data ->> 'agreementVersion' as agreement_version,
		row_number() over
			(partition by aggregate_id order by (tnc_data ->> 'agreementDate')::timestamptz desc) as row_id

    {% endif %}

	from 
		tnc_data_split_cte
),
tnc_data_cte as (
	select
		aggregate_id,
		agreement_date,
		agreement_version
	from 
		tnc_data_normalized
	where
		row_id = 1
)
select
  id,
  brokerage_name,
  first_name,
  last_name,
  email,
  phones,
  adc.aggregate_id,
  created,
  updated,
  zipcount,
  brokeragecode,
  brokerage_email,
  brokerage_phone,
  statelicenses,
  bio,
  mls,
  units,
  sellerpct,
  luxpct,
  buyerpct,
  newpct,
  fthbpct,
  volume,
  trainingcomp,
  experienceyears,
  languagecount,
  unavailabledates,
  reasonforremoval,
  productiontotalunits,
  brokerage,
  removalreason,
  additionalnotes,
  nmlsid,
  receivesreferralsviatextmessage,
  coveredzips,
  closinghours,
  verificationinputfieldsinprogress,
  imagelink,
  statelicenseexpiration,
  trainingcompleted,
  address,
  realestatestartdate,
  termsandconditions,
  verificationstatus,
  cognitousername,
  localtimezone,
  comment,
  coveredareas,
  networkrequirements,
  languages,
  offices,
  office,
  comments,
  inviter_firstname,
  inviter_lastname,
  inviter_email,
  caelegacyagent,
  eligibilitystatus,
  ineligiblecomment,
  productioninput,
  referralagreementemail,
  unverifiedprofilesections,
  unverfiedprofilesections,
  firstonboardingstep,
  profileinfo,
  standardsaccepted,
  reasonfordeactivation,
  workinghours,
  acceptsreferralfees,
  acceptsreferralsonsundays,
  openinghours,
  acceptsreferralsonsaturdays,
  referralcount,
  oldzipcount,
  mra_sent,
  mra_received,
	mra_signed,
	mra_signed_date,
	mra_link,
	mra_comment,
	mra_timestamp,
	mra_user_id,
	mra_user_fullname,
	mra_user_email,
	mra_source,
	agreement_date,
	agreement_version
from
  agent_data_cte adc
	left join mra_data_cte mdc
		on adc.aggregate_id = mdc.aggregate_id
	left join tnc_data_cte tdc
		on adc.aggregate_id = tdc.aggregate_id