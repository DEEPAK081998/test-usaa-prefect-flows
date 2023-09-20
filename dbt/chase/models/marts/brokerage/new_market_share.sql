{{ config(
    materialized = 'table',
    enabled=false
) }}

WITH 
/*
TEST_CTE as(
    select
        lsu.lead_id,
        pup.data->'division'->>'name' AS DIVISIONNAME,
        lsu.data->>'twilioEvent',
        (case when lsu.data->>'twilioEvent' = 'reservation.accepted' THEN 1 END) as CSagentAcceptCount,
        (case when lsu.data->>'twilioEvent' = 'reservation.created'  THEN  1  END) as CSagentcreatedCount

    from lead_status_updates lsu
    join normalized_lead_locations nl on nl.lead_id = lsu.lead_id
    join current_assignments ca on ca.lead_id = lsu.lead_id
    join partner_user_profiles pup on pup.aggregate_id = ca.profile_aggregate_id
    where ca.role = 'HLA'    AND lsu.role = 'AGENT' and pup.data->'division'->>'name' = 'Centralized Sales'
    --group by lsu.lead_id
)
SELECT * FROM TEST_CTE WHERE LEAD_ID = 52322


centralized_sales_lead_count_cte as(
    select
        lsu.lead_id,
        count(case when lsu.data->>'twilioEvent' = 'reservation.accepted' THEN 1 END) as CSagentAcceptCount,
        count(case when lsu.data->>'twilioEvent' = 'reservation.created' THEN  1  END) as CSagentcreatedCount

    from lead_status_updates lsu
    join normalized_lead_locations nl on nl.lead_id = lsu.lead_id
    join current_assignments ca on ca.lead_id = lsu.lead_id
    join partner_user_profiles pup on pup.aggregate_id = ca.profile_aggregate_id
    where ca.role = 'HLA'    AND lsu.role = 'AGENT' and pup.data->'division'->>'name' = 'Centralized Sales'
    group by lsu.lead_id
)
*/
/*partner_user_roles_cte AS (
    select * from partner_user_roles where enabled
)
, partner_user_relationships_cte as (
    SELECT * FROM partner_user_relationships pur
    WHERE pur.enabled = '1'
)
,*/cbsa_cte AS (
    select 
        cbsa.st,
        cbsa.county,
        count(cbsa.zip) as CountyZipCount
    from {{ ref('cbsa_locations') }}cbsa
    group by cbsa.st,cbsa.county
)
/*,brokerage_county_coverage as (
    select
    bcz.brokerage_code,
    cbsa.st,
    cbsa.county,
    count(bcz.zip) as BrokerageCountyZips
    from brokerage_coverage_zips bcz 
    join cbsa_locations cbsa on lpad(cbsa.zip,5,'0') = bcz.zip
    group by bcz.brokerage_code,cbsa.st,cbsa.county
)*/
, partner_user_profiles_CTE AS (
    SELECT 
    distinct
    pup.id,
    pup.aggregate_id as aggregate_id,
    concat(pup.first_name,pup.last_name) as AgentName,
    pup.brokerage_code,
    coalesce(b.full_name,pup.data->'brokerage'->>'fullName') AS brokerage_name,
    pup.verification_status
    FROM {{ ref('partner_user_profiles') }} pup
    left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
)
,agent_count_cte as 
(
    select 
        cbsa.county,
        cbsa.st,
        pup.brokerage_code AS brokerage_code,
        pup.brokerage_name as brokerage_name,
        --pup.verification_status,
        count(distinct puz.profile_id) as agentCount,
        count(distinct case when pup.verification_status = 'Verified-Verified' then puz.profile_id end) as VerifiedagentCount
    from {{ ref('profile_coverage_zips') }} puz
    left join {{ ref('cbsa_locations') }} cbsa on lpad(cbsa.zip,5,'0') = puz.zip
    join partner_user_profiles_CTE pup on pup.id = puz.profile_id
    -- where pup.verification_status = 'Verified-Verified'
   group by cbsa.county, cbsa.st,pup.brokerage_code,brokerage_name --,pup.verification_status --,pup.data->'brokerage'->>'fullName'

)
, final_cte AS (
select lsu.lead_id, lsu.id, lsu.data->>'twilioEvent' as TwilioStatus,
case when lsu.data->>'twilioEvent' = 'reservation.accepted' then 1 else 0 end as Accepted,
case when lsu.data->>'twilioEvent' = 'reservation.created' then 1 else 0 end as Offered,
case when lsu.data->>'twilioEvent' = 'reservation.rejected' then 1 else 0 end as Rejected,
case when lsu.data->>'twilioEvent' = 'reservation.timeout' then 1 else 0 end as Timeout,
pup.aggregate_id as agentAggregateID,
pup.id as agentID,
pup.AgentName,
cbsa.county,
cbsa.st,
cbsa.cbmsa,
COALESCE(acc.agentCount,0) AS agentCount,
COALESCE(acc.VerifiedagentCount,0) AS VerifiedagentCount,
--pup.verification_status as VerificationStatus,
pup.brokerage_code as BrokerageCode,
pup.brokerage_name as BrokerageName,
ed.enrollDate,
coalesce(nl.normalized_sell_location->>'zip',nl.normalized_purchase_location->>'zip') as normalizedZip,
case when t1.transaction_type = 'SELL' then concat('Z',nl.normalized_sell_location->>'zip') else concat('Z',nl.normalized_purchase_location->>'zip') end as stringZip

from {{ ref('lead_status_updates') }} lsu
    join partner_user_profiles_CTE pup on pup.aggregate_id = lsu.profile_aggregate_id
    join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = lsu.lead_id
    join leads t1 on t1.id = lsu.lead_id 
    Left join {{ ref('cbsa_locations') }} cbsa on lpad(cbsa.zip,5,'0') = coalesce(nl.normalized_sell_location->>'zip',nl.normalized_purchase_location->>'zip') 
    left join agent_count_cte acc on
         COALESCE(acc.brokerage_code,'NO-CODE') = COALESCE(pup.brokerage_code,'NO-CODE')
         and acc.st = cbsa.st 
         and acc.county = cbsa.county
         and pup.brokerage_name = acc.brokerage_name
left outer join ( select lead_id, min(created) as enrollDate
                    from {{ ref('lead_status_updates') }} 
                    where category in ('PropertySell','PropertySearch')
                        and lower(status) not like ('invite%')
                    group by lead_id ) ed on ed.lead_id = t1.id
)
--SELECT * FROM final_cte WHERE COUNTY= 'Mobile County'  AND BrokerageCode = 'AL201'
--SELECT * FROM agent_count_cte WHERE COUNTY = 'Mercer County'  AND ST ='NJ'
SELECT * FROM final_cte --WHERE brokerageCode is null and cOUNTY = 'Mercer County' AND ST='NJ' AND BROKERAGENAME IS NOT NULL
--Keller Williams Park Views Realty
/**/