{{
  config(
    materialized = 'view'
    )
}}


WITH  current_lead_statuses_cte AS (
{% if target.type == 'snowflake' %}
  SELECT * FROM (
    SELECT
        lead_id,
        data:closeDate::VARCHAR as CloseDate,
        data:propertyAddress::VARCHAR as PropertyAddress,
        data:salePrice::VARCHAR as PropertyPrice,
        Case
            when lower(data:lender::VARCHAR) like '%chase%' then 'Chase'
            when lower(data:lender::VARCHAR) like '%alliant%' then 'Alliant'
            when lower(data:lender::VARCHAR) like '%freedom%' then 'Freedom'
            when lower(data:lender::VARCHAR) like '%bbva%' then 'BBVA'
            when lower(data:lender::VARCHAR) like '%rbc%' then 'RBC'
            when lower(data:lender::VARCHAR) like '%hsbc%' then 'HSBC'
            when lower(data:lender::VARCHAR) like '%homepoint%' then 'HomePoint'
            when lower(data:lender::VARCHAR) like '%home point%' then 'HomePoint'
            when lower(data:lender::VARCHAR) like '%regions%' then 'Regions'
            when lower(data:lender::VARCHAR) like '%citizens%' then 'Citizens'
            when lower(data:lender::VARCHAR) like '%?%' then 'Unknown'
            else data:lender::VARCHAR end as LenderClosedWith,
        data:agentCommissionPercentage as AgtCommPct,
        updated as updated_at,
        row_number() over(partition by lead_id order by updated desc) as row_num
    FROM {{ ref('current_lead_statuses') }}
    WHERE data:closeDate::VARCHAR is not null --and data->'salePrice' is not null
  ) cte where cte.row_num = 1
{% else %}
  SELECT * FROM (
    SELECT
        lead_id,
        data->>'closeDate' as CloseDate,
        data->>'propertyAddress' as PropertyAddress,
        data->>'salePrice' as PropertyPrice,
        Case
            when lower(data->>'lender') like '%chase%' then 'Chase' 
            when lower(data->>'lender') like '%alliant%' then 'Alliant' 
            when lower(data->>'lender') like '%freedom%' then 'Freedom' 
            when lower(data->>'lender') like '%bbva%' then 'BBVA' 
            when lower(data->>'lender') like '%rbc%' then 'RBC' 
            when lower(data->>'lender') like '%hsbc%' then 'HSBC' 
            when lower(data->>'lender') like '%homepoint%' then 'HomePoint'
            when lower(data->>'lender') like '%home point%' then 'HomePoint' 
            when lower(data->>'lender') like '%regions%' then 'Regions' 
            when lower(data->>'lender') like '%citizens%' then 'Citizens' 
            when lower(data->>'lender') like '%?%' then 'Unknown'  
            else data->>'lender' end as LenderClosedWith,
        data->'agentCommissionPercentage' as AgtCommPct,
        updated as updated_at,
        row_number() over(partition by lead_id order by updated desc) as row_num
    FROM {{ ref('current_lead_statuses') }}
    WHERE data->>'closeDate' is not null --and data->'salePrice' is not null
  ) cte where cte.row_num = 1
{% endif %}
)

, lead_status_updates_cte AS (
{% if target.type == 'snowflake' %}
  SELECT * FROM (
    SELECT
        lsu.lead_id,
        lsu.data:closeDate::VARCHAR as CloseDate,
        lsu.data:propertyAddress::VARCHAR as PropertyAddress,
        lsu.data:salePrice::VARCHAR as PropertyPrice,
        Case
            when lower(lsu.data:lender::VARCHAR) like '%chase%' then 'Chase'
            when lower(lsu.data:lender::VARCHAR) like '%alliant%' then 'Alliant'
            when lower(lsu.data:lender::VARCHAR) like '%freedom%' then 'Freedom'
            when lower(lsu.data:lender::VARCHAR) like '%bbva%' then 'BBVA'
            when lower(lsu.data:lender::VARCHAR) like '%rbc%' then 'RBC'
            when lower(lsu.data:lender::VARCHAR) like '%hsbc%' then 'HSBC'
            when lower(lsu.data:lender::VARCHAR) like '%homepoint%' then 'HomePoint'
            when lower(lsu.data:lender::VARCHAR) like '%home point%' then 'HomePoint'
            when lower(lsu.data:lender::VARCHAR) like '%regions%' then 'Regions'
            when lower(lsu.data:lender::VARCHAR) like '%citizens%' then 'Citizens'
            when lower(lsu.data:lender::VARCHAR) like '%?%' then 'Unknown'
            else lsu.data:lender::VARCHAR end as LenderClosedWith,
        lsu.data:agentCommissionPercentage as AgtCommPct,
        lsu.updated_at as updated_at,
        row_number() over(partition by lead_id order by updated_at desc) as row_num
    FROM {{ ref('lead_status_updates') }} lsu
    WHERE status ='Closed Closed' --AND data->'lender' IS NOT NULL
  ) cte where cte.row_num = 1
{% else %}
  SELECT * FROM (
    SELECT
        lsu.lead_id,
        lsu.data->>'closeDate' as CloseDate,
        lsu.data->>'propertyAddress' as PropertyAddress,
        lsu.data->>'salePrice' as PropertyPrice,
        Case
            when lower(lsu.data->>'lender') like '%chase%' then 'Chase' 
            when lower(lsu.data->>'lender') like '%alliant%' then 'Alliant' 
            when lower(lsu.data->>'lender') like '%freedom%' then 'Freedom' 
            when lower(lsu.data->>'lender') like '%bbva%' then 'BBVA' 
            when lower(lsu.data->>'lender') like '%rbc%' then 'RBC' 
            when lower(lsu.data->>'lender') like '%hsbc%' then 'HSBC' 
            when lower(lsu.data->>'lender') like '%homepoint%' then 'HomePoint'
            when lower(lsu.data->>'lender') like '%home point%' then 'HomePoint' 
            when lower(lsu.data->>'lender') like '%regions%' then 'Regions' 
            when lower(lsu.data->>'lender') like '%citizens%' then 'Citizens' 
            when lower(lsu.data->>'lender') like '%?%' then 'Unknown'  
            else lsu.data->>'lender' end as LenderClosedWith,
        lsu.data->'agentCommissionPercentage' as AgtCommPct,
        lsu.updated_at as updated_at,
        row_number() over(partition by lead_id order by updated_at desc) as row_num
    FROM {{ ref('lead_status_updates') }} lsu
    WHERE status ='Closed Closed' --AND data->'lender' IS NOT NULL
  ) cte where cte.row_num = 1
{% endif %}
)

, base_cte AS (
  SELECT * FROM current_lead_statuses_cte
  UNION ALL
  SELECT * FROM lead_status_updates_cte WHERE lead_id not in (select lead_id from current_lead_statuses_cte)
)
, base_dates_cte as (
  SELECT 
  case 
    when closedate like '%T%' then closedate
  end as closedatewithtimestamp
  ,case
    {% if target.type == 'snowflake' %}
    when closedate like '%-%' and  closedate not like '%T%' then regexp_replace(closedate, '[^(\\d\\-)]', '', '1', '0')
    {% else %}
    when closedate like '%-%' and  closedate not like '%T%' then regexp_replace(closedate, '[^(\d\-)]', '', 'g')
    {% endif %}
  end as closedateEU
  ,
  case 
    when closedate like '%,%' then closedate
  end as closedateString,
  case 
    when closedate like '%/%' then closedate
  end as closedateStringdash,
  case when closedate not like '%-%' and  closedate not like '%T%' and closedate not like '%,%' and closedate not like '%/%' then closedate::bigint/1000
  end closedatetimestamp,
  * FROM base_cte
)
, final_cte as (
  select
  {% if target.type == 'snowflake' %}
  coalesce(cast(closedatewithtimestamp as date),cast(closedateStringdash as date), cast(TO_TIMESTAMP(closedatestring, 'MON DD, YYYY HH12:MI:SS AM') as date) ) as new_close_date,
  {% else %}
  coalesce(cast(closedatewithtimestamp as date),cast(closedateStringdash as date),cast(closedatestring as date) ) as new_close_date,
  {% endif %}
  substring(closedateEU,1,4) as eu_year,
  case 
  	when length(substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))) >=2 and substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))::int > 12 
  	then substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))
  	{% if target.type == 'snowflake' %}
  	else regexp_replace(right(closedateEU,2), '[^(\\d)]', '', '1', '0')
  	{% else %}
  	else regexp_replace(right(closedateEU,2), '[^(\d)]', '', 'g')
  	{% endif %}
  end as eu_month,
  case 
  	when length(substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))) >=2 and substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))::int > 12 
  	{% if target.type == 'snowflake' %}
  	then regexp_replace(right(closedateEU,2), '[^(\\d)]', '', '1', '0')
  	{% else %}
  	then regexp_replace(right(closedateEU,2), '[^(\d)]', '', 'g')
  	{% endif %}
  	when length(substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))) <2 then '0'||substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU)))) 
    else substring(closedateEU,6,position('-' in substring(closedateEU,7,length(closedateEU))))
  end as eu_day,
  to_timestamp(closedatetimestamp)::date as newclosedatetimestamp,
  * from base_dates_cte
)
SELECT
lead_id,
coalesce(new_close_date,newclosedatetimestamp,(eu_year||'-'||eu_day||'-'||eu_month)::date) as CloseDate,
PropertyAddress,
PropertyPrice,
LenderClosedWith,
AgtCommPct,
updated_at,
row_num
 FROM final_cte