{{
  config(
    materialized = 'table',
    )
}}
WITH

agent_count_cte AS (
    select
        cb.county,
        COALESCE(pup.brokerage_code,'NO-BROKERAGE-CODE') AS brokerage_code,
        cb.st,
        count(distinct puz.profile_id) as agentCount,
        count(distinct case when pup.brokerage_code is not null then puz.profile_id end) as BAagentCount,
        count(distinct case when pup.brokerage_code is null then puz.profile_id end) as unaffiliatedagentCount
    from {{ ref('profile_coverage_zips') }} puz
    join {{ source('public','cbsa_locations') }} cb on lpad(cb.zip,5,'0') = puz.zip
    join {{ ref('partner_user_profiles') }}  pup on pup.id = puz.profile_id
    where pup.verification_status = 'Verified-Verified'
    group by cb.county, cb.st, pup.brokerage_code
)
, unaffiliated_agent_count_cte AS (
    select 
        cb.county,
        cb.st,
        count(distinct case when pup.brokerage_code is null then puz.profile_id end) as unaffiliatedagentCountt
    from {{ ref('profile_coverage_zips') }} puz
    join {{ source('public','cbsa_locations') }} cb on lpad(cb.zip,5,'0') = puz.zip
    join {{ ref('partner_user_profiles') }}  pup on pup.id = puz.profile_id
    where pup.verification_status = 'Verified-Verified'
    group by cb.county, cb.st
)
--select * from agent_count_cte where st ='ID'
,leads_by_profile  as (
select lead_id,profile_aggregate_id from {{ ref('current_assignments') }} 
where ROLE ilike '%brok%'
group by 1,2
)
, leads_by_brokerage AS (

select distinct lbp.lead_id ,b.brokerage_code from {{ ref('brokerages') }} b 
join leads_by_profile lbp
on b.aggregate_id = lbp.profile_aggregate_id

)
, dates_cte AS (
    select lead_id, min(created) as enrollDate
    from {{ ref('lead_status_updates') }} 
    where category in ('PropertySell','PropertySearch') and lower(status) not like ('invite%')
    group by lead_id
)
, lead_count_cte AS (
    select 
        cb.county,
        cb.st,
   --     cte.brokerage_code,
        count(distinct case when current_date - date(dc.enrollDate - interval '5 Hours') < 181  then t1.id end) as leadCount180,
        count(distinct case when current_date - date(dc.enrollDate - interval '5 Hours') < 366  then t1.id end) as leadCount365
    from {{ ref('leads') }}  t1
    join {{ ref('normalized_lead_locations') }}  nl on nl.lead_id = t1.id
    join dates_cte dc on dc.lead_id = t1.id
    join {{ source('public','cbsa_locations') }} cb 
        on
        lpad(cb.zip,5,'0') = COALESCE(nl.normalized_sell_location::json->>'zip',nl.normalized_purchase_location::json->>'zip')
    --join leads_by_brokerage cte on t1.id = cte.lead_id
    group by cb.county, cb.st --,cte.brokerage_code
)
--select count(*) from leads_by_brokerage where brokerage_code='ID302' -- where st='ID'
, brokarage_coverage_zip_cte AS (
    select 
        cb.county,
        cb.st,
        bcz.brokerage_code,
        count(distinct bcz.brokerage_code) as brokeragecount
    from {{ ref('brokerage_coverage_zips') }}  bcz
    join {{ source('public','cbsa_locations') }} cb on lpad(cb.zip,5,'0') = bcz.zip
    group by cb.county,cb.st,brokerage_code
)
select 
cbsa.st,
cbsa.county,
cbsa.brokerage_code,
/*case when count(cbsa.brokerage_code) = 0 then 'No Coverage'
     when count(cbsa.brokerage_code) = 1 then 'Low Coverage'
     when count(cbsa.brokerage_code) = 2 then 'Good Coverage'
     when count(cbsa.brokerage_code) >= 3 then 'High Coverage' end as County_Coverage,*/
case when ac.agentCount is null then 0 else ac.agentCount end as agentCount,
bac.BAagentCount,
bac.unaffiliatedagentCount,
uac.unaffiliatedagentCountt as unw,
uac.unaffiliatedagentCountt::float/COUNT(1) OVER(PARTITION BY cbsa.st,cbsa.county) as unaffiliatedCount,
lc.leadCount180 as leadCount180_unw,
lc.leadCount180::float / COUNT(1) OVER(PARTITION BY cbsa.st,cbsa.county) as leadCount180,
lc.leadCount365 as LeadCount365_unw ,
lc.leadCount365 / COUNT(1) OVER(PARTITION BY cbsa.st,cbsa.county) as leadCount365,
cbsa.brokeragecount,
sum(cbsa.brokeragecount) OVER(PARTITION BY cbsa.st,cbsa.county) as totalBrokerCountbyCounty
FROM brokarage_coverage_zip_cte cbsa
left outer join agent_count_cte ac on ac.county = cbsa.county and ac.st = cbsa.st and ac.brokerage_code = cbsa.brokerage_code
left outer join agent_count_cte bac on bac.county = cbsa.county and bac.st = cbsa.st and bac.brokerage_code = cbsa.brokerage_code
left outer join unaffiliated_agent_count_cte uac on uac.county = cbsa.county and uac.st = cbsa.st
left outer join lead_count_cte lc on lc.county = cbsa.county and lc.st = cbsa.st -- and lc.brokerage_code=cbsa.brokerage_code
-- where cbsa.county = 'Cook County'
/**/