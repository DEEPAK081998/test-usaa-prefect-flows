{{
  config(
    materialized = 'view',
    unique_key = 'lead_id',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
	    {'columns': ['updated_at'], 'type': 'btree'},
      {'columns': ['HB_Status']},
	  ],
    )
}}

WITH
source_pending_cte AS (
    SELECT lead_id, max(id) as id, max(updated_at) as updated_at
    FROM {{ ref('lead_status_updates') }}
    {% if target.type == 'snowflake' %}
    WHERE data:closeDate is not null and category in ('PropertySell','PropertySearch') and status like 'Pending%'
    {% else %}
    WHERE data->'closeDate' is not null and category in ('PropertySell','PropertySearch') and status like 'Pending%'
    {% endif %}
    GROUP BY lead_id
)
,lead_current_status_pending AS (
  {# replace stg_lead_current_status_pending #}
  SELECT
      {% if target.type == 'snowflake' %}
      l.lead_id, data:closeDate::VARCHAR as PendingCloseDate,
      l.data:propertyAddress::VARCHAR as PendingPropertyAddress,
      {% else %}
      l.lead_id, data->>'closeDate' as PendingCloseDate,
      l.data->>'propertyAddress' as PendingPropertyAddress,
      {% endif %}
      n.updated_at
  FROM source_pending_cte n
  JOIN {{ ref('lead_status_updates') }} l
  ON l.lead_id = n.lead_id and l.id = n.id
)
,source_close_cte as (
    SELECT  lead_id, max(id) as id, max(updated_at) as updated_at
    FROM {{ ref('lead_status_updates') }}
    {% if target.type == 'snowflake' %}
    WHERE data:closeDate is not null  and category in ('HomeFinancing') and status = 'Closed Closed'
    {% else %}
    WHERE data->'closeDate' is not null  and category in ('HomeFinancing') and status = 'Closed Closed'
    {% endif %}
    group by lead_id
)
,lead_current_status_closed AS (
  {# replace stg_lead_current_status_closed #}
  SELECT
    m.lead_id,
    {% if target.type == 'snowflake' %}
    m.data:closeDate::VARCHAR as FinanceCloseDate,
    {% else %}
    m.data->>'closeDate' as FinanceCloseDate,
    {% endif %}
    l.updated_at
  FROM source_close_cte l
  JOIN {{ ref('lead_status_updates') }} m on l.lead_id = m.lead_id and l.id = m.id
)

select
{% if target.type != 'snowflake' %}
  distinct on (a.lead_id)
{% endif %}
                a.lead_id,
                a.MLO_Status,
                a.MLO_Status_Time,
                a.RC_Status,
                a.RC_Status_Time,
                a.Agent_Status,
                a.Agent_Status_Time,
                a.HB_Status,
                a.HB_Status_time,
                a.Category,
                b.CloseDate::date as lsuCloseDate,
                e.CloseDate::date as CloseDate,
                b.PropertyAddress,
                b.PropertyPrice,
                coalesce(e.LenderClosedWith,b.LenderClosedWith) as LenderClosedWith,
                b.AgtCommPct,
                e.date_marked_closed_final,
                e.max_date,
                c.PendingCloseDate,
                c.PendingPropertyAddress,
                d.FinanceCloseDate,
                a.updated_at
from {{ ref('stg_lead_current_lead_statuses_agg') }} a
left outer join lead_current_status_pending c on c.lead_id = a.lead_id
left outer join lead_current_status_closed d on d.lead_id = a.lead_id
left outer join {{ ref('stg_lead_current_lead_statuses_not_closed') }} b on b.lead_id = a.lead_id
left join {{ ref('stg_lead_lenderclosewith') }} e on a.lead_id = e.lead_id
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.lead_id ORDER BY a.lead_id) = 1
{% endif %}
--  {% if is_incremental() %}
--		{% if not var('incremental_catch_up') %}
--    WHERE a.updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01') + interval '-2 day'
--    {% else %}
--    WHERE a.updated_at >= CURRENT_DATE + interval '-60 day'
--    {% endif %}
--  {% endif %}