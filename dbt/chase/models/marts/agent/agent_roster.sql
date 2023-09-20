{{
  config(
    materialized = 'table',
    enabled=false
    )
}}
WITH 

select pup.id, pup.aggregate_id, pup.first_name, pup.last_name, pup.email, op.agentOfficePhone, mp.agentMobilePhone,
       pup.data->'brokerage'->>'brokerageCode' as brokerageCode,
       case when pup.data->'brokerage'->>'brokerageCode' isnull then pup.data->'brokerage'->>'email' else b.data->>'email' end as brokerageEmail,
       case when pup.data->'brokerage'->>'brokerageCode' isnull then pup.data->'brokerage'->>'fullName' else b.full_name end as brokerageName,
       pup.data,ar.role, 
       case when pup.verification_status = 'Verified-Provisional' then 'Verified-Verified'
    when pup.verification_status = 'Verified-Select Only' then 'Verified-Verified'
    else pup.verification_status end as verification_status,
    pup.verification_status as internal_verificationstatus, pup.created, pup.data->>'inviter' as agentInviter,
       CASE WHEN pup.data->>'caeLegacyAgent'= 'true' THEN 'true' ELSE 'false' END as cae_legacy_agent,
       CASE WHEN lower(pup.data->'inviter'->>'email') LIKE '%chase%' THEN 'true' ELSE 'false' END as hla_invited_agent,  
       CASE WHEN (pup.data->>'caeLegacyAgent' <> 'true' OR pup.data->>'caeLegacyAgent' IS NULL) AND pup.data->'inviter'->>'email' IS NULL AND pup.brokerage_code IS NULL THEN 'true'
      ELSE 'false' END AS self_enroll_flag,
      pup.data->'profileInfo'->'totalHomesClosed' ->>'totalNumberOfHomesClosed24Months' as Units,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageHomesClosedForSellers' as sellerpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageLuxuryHomeClosings' as luxpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageHomesClosedForBuyers' as buyerpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageNewConstructionClosings' as newpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'percentageFirstTimeHomeBuySellClosing' as fthbpct,
    pup.data->'profileInfo'->'totalHomesClosed'->>'totalHomesClosedVolume' as volume,
    pup.data->>'productionTotalUnits' as productionTotalUnits,
    rc.RC_Name,
    rc.RC_Email,
    rc.RC_Phone,
    b.enabled,
    z.coverageCount
from partner_user_profiles pup
join (
    select user_profile_id, role from partner_user_roles where lower(role) = 'agent'
    ) ar on pup.id = ar.user_profile_id
left outer join (select id, min(PhoneNumber) as agentOfficePhone
            from (select id, json_array_elements(phones)->>'phoneType' as PhoneType,
                        json_array_elements(phones)->>'phoneNumber' as phoneNumber
                    from partner_user_profiles) pop
                    where lower(pop.Phonetype) = 'office'
                group by id) op on pup.id = op.id
left outer join (select id, min(PhoneNumber) as agentMobilePhone
            from (select id, json_array_elements(phones)->>'phoneType' as PhoneType,
                        json_array_elements(phones)->>'phoneNumber' as phoneNumber
                    from partner_user_profiles) pop
                    where lower(pop.Phonetype) = 'mobilephone'
                group by id) mp on pup.id = mp.id
left join (select b.brokerage_code,b.enabled,
            concat(pup.first_name,' ',pup.last_name) as RC_Name,
            pup.email as RC_Email,
            pup.phones as RC_Phone
            from brokerages b
            join partner_user_relationships pur on b.aggregate_id = pur.parent_profile_id
            join partner_user_profiles pup on pur.child_profile_id = pup.aggregate_id
          where pur.enabled = '1') rc on rc.brokerage_code = pup.brokerage_code
left join (select brokerage_code, count(zip) as coverageCount 
             from brokerage_coverage_zips 
             group by brokerage_code) z on z.brokerage_code = pup.brokerage_code
left join brokerages b on b.brokerage_code = pup.brokerage_code