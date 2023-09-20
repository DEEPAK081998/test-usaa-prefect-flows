{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
      {'columns': ['updated_at'], 'type': 'btree'},
	],
    )
}}
WITH
cte AS (
    SELECT lead_id, max(id) as id, NOW() as updated_at
    FROM {{ ref('lead_status_updates') }} 
    WHERE data::json->'closeDate' is not null 
    and category in ('PropertySell','PropertySearch') and status like 'Pending%'--and data::json->'salePrice' is not null
    {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
        and created >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
        {% else %}
        and created >= CURRENT_DATE + interval '-60 day'
        {% endif %}
    {% endif %}
    GROUP BY lead_id
)
SELECT
    l.lead_id, data::json->>'closeDate' as PendingCloseDate,
    l.data::json->>'propertyAddress' as PendingPropertyAddress,
    n.updated_at
FROM cte n
JOIN {{ ref('lead_status_updates') }}  l
ON l.lead_id = n.lead_id and l.id = n.id
