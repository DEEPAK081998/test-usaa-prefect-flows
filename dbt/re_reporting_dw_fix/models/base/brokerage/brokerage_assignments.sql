{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_aggregate_id'], 'type': 'hash'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','brokerage_assignments_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 cast(lead_aggregate_id as {{ uuid_formatter() }}) as lead_aggregate_id,
	 brokerage_code,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated
FROM {{source('public', 'raw_import_brokerage_assignments')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}