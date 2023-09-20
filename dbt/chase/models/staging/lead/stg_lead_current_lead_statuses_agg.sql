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
SELECT
    lead_id,
    max(category) as Category,
    max(case when role = 'MLO' then status end) as MLO_Status,
    (max(case when role = 'MLO' then created end)-interval '7 hour') as MLO_Status_Time,
    max(case when role = 'REFERRAL_COORDINATOR' and category in ('PropertySell','PropertySearch') then status end) as RC_Status,
    (max(case when role = 'REFERRAL_COORDINATOR' and category in ('PropertySell','PropertySearch') then created end)-interval '7 hour') as RC_Status_Time,
    max(case when role = 'AGENT' and category in ('PropertySell','PropertySearch') then status end) as Agent_Status,
    (max(case when role = 'AGENT' and category in ('PropertySell','PropertySearch') then created end)-interval '7 hour') as Agent_Status_Time,
    max(case when category in ('PropertySell','PropertySearch') then status end) as HB_Status,
    (max(case when category in ('PropertySell','PropertySearch') then created end)-interval '7 hour') as HB_Status_Time,
    max(updated) AS updated_at
FROM {{ ref('current_lead_statuses') }} 
  {% if is_incremental() %}
		{%- if not var('incremental_catch_up') %}
    WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% else %}
    WHERE updated >= CURRENT_DATE + interval '-60 day'
    {% endif %}
  {% endif %}
GROUP BY lead_id
