WITH
zip_eligibility_status_count AS (
  SELECT
    pcz.zip,
    pup.eligibility_status,
    count(distinct pup.id) as agentCount
  FROM {{ ref('profile_coverage_zips') }} pcz
  JOIN {{ ref('partner_user_profiles') }} pup on pcz.profile_id = pup.id
  JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
  WHERE pur.role='AGENT'
  GROUP BY pcz.zip, pup.eligibility_status
)
,brokerage_count_cte AS (
  SELECT bcz.zip, count(distinct brokerage_code) as brokeragecount
  FROM {{ ref('brokerage_coverage_zips') }} bcz
  GROUP BY bcz.zip
)
, lead_status_cte AS (
{% if target.type == 'snowflake' %}
  SELECT
    case
      when nl.normalized_sell_location:zip::VARCHAR is null then nl.normalized_purchase_location:zip::VARCHAR
      else nl.normalized_sell_location:zip::VARCHAR
    end as Zip,
    count(distinct lsu.lead_id) as leadCount
  FROM {{ ref('lead_status_updates') }} lsu
  JOIN {{ ref('normalized_lead_locations') }} nl on nl.lead_id = lsu.lead_id
  WHERE category in ('PropertySell','PropertySearch') AND lower(status) not like ('invite%')
  GROUP BY nl.normalized_sell_location:zip::VARCHAR, nl.normalized_purchase_location:zip::VARCHAR
  HAVING DATEDIFF(DAY, min(lsu.created), current_date) < 181
{% else %}
  SELECT
    case
      when nl.normalized_sell_location->>'zip' is null then nl.normalized_purchase_location->>'zip' 
      else nl.normalized_sell_location->>'zip'
    end as Zip,
    count(distinct lsu.lead_id) as leadCount
  FROM {{ ref('lead_status_updates') }} lsu 
  JOIN {{ ref('normalized_lead_locations') }} nl on nl.lead_id = lsu.lead_id
  WHERE category in ('PropertySell','PropertySearch') AND lower(status) not like ('invite%')
  GROUP BY nl.normalized_sell_location->>'zip',nl.normalized_purchase_location->>'zip'
  HAVING DATE_PART('day', current_date - min(lsu.created)) < 181
{% endif %}
)
{% if target.type == 'snowflake' %}
SELECT
  lpad(cbsa.zip,5,'0') as zip,
  cbsa.county,
  cbsa.cbmsa,
  cbsa.st as state,
  bc.brokeragecount,
  ac.agentCount,
  lc.leadCount
from {{ ref('cbsa_locations') }} cbsa
left outer join zip_eligibility_status_count ac on ac.zip = lpad(cbsa.zip,5,'0')
left outer join brokerage_count_cte bc on bc.zip = lpad(cbsa.zip,5,'0')
left outer join lead_status_cte lc on lc.Zip = lpad(cbsa.zip,5,'0')
QUALIFY ROW_NUMBER() OVER (PARTITION BY (lpad(cbsa.zip,5,'0')) ORDER BY (lpad(cbsa.zip,5,'0'))) = 1
{% else %}
SELECT
  distinct on (lpad(cbsa.zip,5,'0')) lpad(cbsa.zip,5,'0') as zip,
  cbsa.county,
  cbsa.cbmsa,
  cbsa.st as state,
  bc.brokeragecount,
  ac.agentCount,
  lc.leadCount
from {{ ref('cbsa_locations') }} cbsa
left outer join zip_eligibility_status_count ac on ac.zip = lpad(cbsa.zip,5,'0')
left outer join brokerage_count_cte bc on bc.zip = lpad(cbsa.zip,5,'0')
left outer join lead_status_cte lc on lc.Zip = lpad(cbsa.zip,5,'0')
{% endif %}