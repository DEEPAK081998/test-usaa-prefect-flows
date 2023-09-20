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
SELECT lead_id, min(created) as inviteDate, NOW() as updated_at
FROM {{ ref('lead_status_updates') }}  
WHERE category in ('PropertySell','PropertySearch')  and lower(status) like ('invite%')
    {% if is_incremental() %}
		{% if not var('incremental_catch_up') %}
        and created >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
        {% else %}
        and created >= CURRENT_DATE + interval '-60 day'
        {% endif %}
    {% endif %}
GROUP BY lead_id