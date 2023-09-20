WITH
zip_eligibility_status_count AS (
  SELECT
    pcz.zip,
    pup.eligibility_status,
    count(distinct pup.id) as agentCount
  FROM {{ ref('profile_coverage_zips') }} pcz
  JOIN {{ ref('partner_user_profiles') }}  pup on pcz.profile_id = pup.id
  JOIN {{ ref('partner_user_roles') }}  pur on pur.user_profile_id = pup.id
  WHERE pur.role='AGENT'
  GROUP BY pcz.zip, pup.eligibility_status
)
,brokerage_count_cte AS (
  SELECT bcz.zip, count(distinct brokerage_code) as brokeragecount
  FROM {{ ref('brokerage_coverage_zips') }}  bcz
  GROUP BY bcz.zip
)
, lead_status_cte AS (
  SELECT
    case
      when nl.normalized_sell_location::json->>'zip' is null then nl.normalized_purchase_location::json->>'zip' 
      else nl.normalized_sell_location::json->>'zip'
    end as Zip,
    count(distinct lsu.lead_id) as leadCount
  FROM {{ ref('lead_status_updates') }}  lsu
  JOIN {{ ref('normalized_lead_locations') }}  nl on nl.lead_id = lsu.lead_id
  WHERE category in ('PropertySell','PropertySearch') AND lower(status) not like ('invite%')
  GROUP BY nl.normalized_sell_location::json->>'zip',nl.normalized_purchase_location::json->>'zip'
  HAVING DATE_PART('day', current_date - min(lsu.created)) < 181
)
SELECT
  distinct on (lpad(cbsa.zip,5,'0')) lpad(cbsa.zip,5,'0') as zip,
  cbsa.county,
  cbsa.cbmsa,
  cbsa.st as state,
  bc.brokeragecount,
  ac.agentCount,
  lc.leadCount
from {{ source('public','cbsa_locations') }} cbsa
left outer join zip_eligibility_status_count ac on ac.zip = lpad(cbsa.zip,5,'0')
left outer join brokerage_count_cte bc on bc.zip = lpad(cbsa.zip,5,'0')
left outer join lead_status_cte lc on lc.Zip = lpad(cbsa.zip,5,'0')