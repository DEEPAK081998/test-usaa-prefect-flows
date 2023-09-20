{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    )
}}
WITH
cte as (
    SELECT  lead_id, max(id) as id, max(updated_at) as updated_at
    FROM {{ ref('lead_status_updates') }}
    {% if target.type == 'snowflake' %}
    WHERE data:closeDate is not null  and category in ('HomeFinancing') and status = 'Closed Closed'
    {% else %}
    WHERE data->'closeDate' is not null  and category in ('HomeFinancing') and status = 'Closed Closed'
    {% endif %}
    {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
        and updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01') + interval '-2 day'
        {% else %}
        and updated_at >= CURRENT_DATE + interval '-60 day'
        {% endif %}
    {% endif %}
    group by lead_id
)
SELECT
    m.lead_id,
    {% if target.type == 'snowflake' %}
    m.data:closeDate::VARCHAR as FinanceCloseDate,
    {% else %}
    m.data->>'closeDate' as FinanceCloseDate,
    {% endif %}
    l.updated_at
FROM cte l
JOIN {{ ref('lead_status_updates') }} m on l.lead_id = m.lead_id and l.id = m.id