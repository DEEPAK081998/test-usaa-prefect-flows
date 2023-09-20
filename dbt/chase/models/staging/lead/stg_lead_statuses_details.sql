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
select  distinct on (a.lead_id) a.lead_id,
                a.MLO_Status,
                a.MLO_Status_Time,
                a.RC_Status,
                a.RC_Status_Time,
                a.Agent_Status,
                a.Agent_Status_Time,
                a.HB_Status,
                a.HB_Status_time,
                a.Category,
                b.CloseDate,
                b.PropertyAddress,
                b.PropertyPrice,
                b.LenderClosedWith,
                b.AgtCommPct,
                c.PendingCloseDate,
                c.PendingPropertyAddress,
                d.FinanceCloseDate,
                a.updated_at
from {{ ref('stg_lead_current_lead_statuses_agg') }} a
left outer join {{ ref('stg_lead_current_status_pending') }}  c on c.lead_id = a.lead_id
left outer join {{ ref('stg_lead_current_status_closed') }} d on d.lead_id = a.lead_id
left outer join {{ ref('stg_lead_current_lead_statuses_not_closed') }} b on b.lead_id = a.lead_id
  {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
    WHERE a.updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% else %}
    WHERE a.updated_at >= CURRENT_DATE + interval '-60 day'
    {% endif %}
  {% endif %}