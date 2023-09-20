{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    )
}}


WITH  current_lead_statuses_cte AS (
SELECT
    lead_id,
    data::json->>'closeDate' as CloseDate,
    data::json->>'propertyAddress' as PropertyAddress,
    data::json->>'salePrice' as PropertyPrice,
    Case
        when lower(data::json->>'lender') like '%chase%' then 'Chase'
        when lower(data::json->>'lender') like '%alliant%' then 'Alliant'
        when lower(data::json->>'lender') like '%freedom%' then 'Freedom'
        when lower(data::json->>'lender') like '%bbva%' then 'BBVA'
        when lower(data::json->>'lender') like '%rbc%' then 'RBC'
        when lower(data::json->>'lender') like '%hsbc%' then 'HSBC'
        when lower(data::json->>'lender') like '%homepoint%' then 'HomePoint'
        when lower(data::json->>'lender') like '%home point%' then 'HomePoint'
        when lower(data::json->>'lender') like '%regions%' then 'Regions'
        when lower(data::json->>'lender') like '%citizens%' then 'Citizens'
        when lower(data::json->>'lender') like '%?%' then 'Unknown'
        else data::json->>'lender' end as LenderClosedWith,
    data::json->'agentCommissionPercentage' as AgtCommPct,
    updated as updated_at
FROM {{ ref('current_lead_statuses') }} 
WHERE data::json->>'closeDate' is not null --and data::json->'salePrice' is not null
{% if is_incremental() %}
	{% if not var('incremental_catch_up') %}
        and updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
  {% else %}
        and updated >= CURRENT_DATE + interval '-60 day'
  {% endif %}
{% endif %}
)

, lead_status_updates_cte AS (

SELECT
    lsu.lead_id,
    lsu.data::json->>'closeDate' as CloseDate,
    lsu.data::json->>'propertyAddress' as PropertyAddress,
    lsu.data::json->>'salePrice' as PropertyPrice,
    Case
        when lower(lsu.data::json->>'lender') like '%chase%' then 'Chase'
        when lower(lsu.data::json->>'lender') like '%alliant%' then 'Alliant'
        when lower(lsu.data::json->>'lender') like '%freedom%' then 'Freedom'
        when lower(lsu.data::json->>'lender') like '%bbva%' then 'BBVA'
        when lower(lsu.data::json->>'lender') like '%rbc%' then 'RBC'
        when lower(lsu.data::json->>'lender') like '%hsbc%' then 'HSBC'
        when lower(lsu.data::json->>'lender') like '%homepoint%' then 'HomePoint'
        when lower(lsu.data::json->>'lender') like '%home point%' then 'HomePoint'
        when lower(lsu.data::json->>'lender') like '%regions%' then 'Regions'
        when lower(lsu.data::json->>'lender') like '%citizens%' then 'Citizens'
        when lower(lsu.data::json->>'lender') like '%?%' then 'Unknown'
        else lsu.data::json->>'lender' end as LenderClosedWith,
    lsu.data::json->'agentCommissionPercentage' as AgtCommPct,
    NOW() as updated_at
FROM {{ ref('lead_status_updates') }}  lsu
WHERE status ='Closed Closed' AND data::json->'lender' IS NOT NULL

)


, final_cte AS (

  SELECT * FROM current_lead_statuses_cte WHERE lead_id not in (select lead_id from lead_status_updates_cte)
  UNION ALL
  SELECT * FROM lead_status_updates_cte WHERE lead_id not in (select lead_id from current_lead_statuses_cte)
)

SELECT * FROM final_cte