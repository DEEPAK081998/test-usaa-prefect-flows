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
SELECT
t1.*,
t1.updated as updated_at,
bn.bank_name
FROM
{{ ref('leads') }}  t1
 join {{ ref('stg_lead_banks') }} bn on t1.bank_id = bn.bank_id
WHERE
 t1.id not in {{ TestLeads() }}
  and t1.bank_id not in ('25A363D4-0BE0-4001-B9FB-A2F8BA91170C')
  and t1.created > '2017-09-01'::date
  {% if is_incremental() %}
		{%- if not var('incremental_catch_up') %}
    and t1.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% else %}
    and t1.updated >= CURRENT_DATE + interval '-60 day'
    {% endif %}
  {% endif %}
