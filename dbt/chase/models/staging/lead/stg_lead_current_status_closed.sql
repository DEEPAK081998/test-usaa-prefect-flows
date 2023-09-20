{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id'
    )
}}
WITH
cte as (
    SELECT  lead_id, max(id) as id, NOW() as updated_at
    FROM {{ ref('lead_status_updates') }} 
    WHERE data::json->'closeDate' is not null  and category in ('HomeFinancing') and status = 'Closed Closed'
    {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
        and created >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
        {% else %}
        and created >= CURRENT_DATE + interval '-60 day'
        {% endif %}
    {% endif %}
    group by lead_id
)
SELECT
    m.lead_id,
    m.data::json->>'closeDate' as FinanceCloseDate,
    l.updated_at
FROM cte l
JOIN {{ ref('lead_status_updates') }}  m on l.lead_id = m.lead_id and l.id = m.id