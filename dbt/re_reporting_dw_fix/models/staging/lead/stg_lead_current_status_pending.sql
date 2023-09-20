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
    SELECT lead_id, max(id) as id, max(updated_at) as updated_at
    FROM {{ ref('lead_status_updates') }}
    {% if target.type == 'snowflake' %}
    WHERE data:closeDate is not null
    {% else %}
    WHERE data->'closeDate' is not null
    {% endif %}
    and category in ('PropertySell','PropertySearch') and status like 'Pending%'--and data->'salePrice' is not null
    {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
        and updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')+ interval '-2 day'
        {% else %}
        and updated_at >= CURRENT_DATE + interval '-60 day'
        {% endif %}
    {% endif %}
    GROUP BY lead_id
)
SELECT
    {% if target.type == 'snowflake' %}
    l.lead_id, data:closeDate::VARCHAR as PendingCloseDate,
    l.data:propertyAddress::VARCHAR as PendingPropertyAddress,
    {% else %}
    l.lead_id, data->>'closeDate' as PendingCloseDate,
    l.data->>'propertyAddress' as PendingPropertyAddress,
    {% endif %}
    n.updated_at
FROM cte n
JOIN {{ ref('lead_status_updates') }} l
ON l.lead_id = n.lead_id and l.id = n.id
