{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
	    {'columns': ['updated_at'], 'type': 'btree'},
      {'columns': ['HB_Status']},
	  ],
    )
}}
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
                {% if target.type == 'snowflake' %}
                TO_TIMESTAMP_NTZ(b.CloseDate)::DATE as lsuCloseDate,
                TO_TIMESTAMP_NTZ(b.CloseDate)::DATE as CloseDate,
                {% else %}
                b.CloseDate::date as lsuCloseDate,
                e.CloseDate::date as CloseDate,
                {% endif %}
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
left outer join {{ ref('stg_lead_current_status_pending') }}  c on c.lead_id = a.lead_id
left outer join {{ ref('stg_lead_current_status_closed') }} d on d.lead_id = a.lead_id
left outer join {{ ref('stg_lead_current_lead_statuses_not_closed') }} b on b.lead_id = a.lead_id
left join {{ ref('stg_lead_lenderclosewith') }} e on a.lead_id = e.lead_id
  {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
    WHERE a.updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01') + interval '-2 day'
    {% else %}
    WHERE a.updated_at >= CURRENT_DATE + interval '-60 day'
    {% endif %}
  {% endif %}
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.lead_id ORDER BY a.lead_id) = 1
{% endif %}
