{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    indexes=[
      {'columns': ['id'], 'type': 'hash'},
      {'columns': ['updated_at'], 'type': 'btree'},
	],
    )
}}
WITH
cte AS (

    SELECT distinct on (lead_id) lead_id, min(created) as inactiveDate, role as inactiveRole,NOW() as updated_at
    FROM {{ ref('lead_status_updates') }} 
    WHERE category in ('PropertySell','PropertySearch','ConciergeStatus') and status like 'Inactive%'
    {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
        and created >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
        {% else %}
        and created >= CURRENT_DATE + interval '-60 day'
        {% endif %}
    {% endif %}
    GROUP BY lead_id, role
)
SELECT
    l.id,
    ind.inactiveDate,
    ind.inactiveRole,
    DATE_PART('day', ind.inactiveDate - l.created) as timetoInactive,
    COALESCE(ind.updated_at,l.updated) as updated_at
FROM {{ ref('leads') }}  l
left outer join cte ind 
on l.id = ind.lead_id

