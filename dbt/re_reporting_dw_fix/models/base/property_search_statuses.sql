{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['status'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','property_search_statuses_pkey') }}")
	]
) }}
select
	 cast(id as bigint) as id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 status,
	 cast(update_id as {{ uuid_formatter() }}) as update_id,
	 cast(lead_id as bigint) as lead_id,
	 category,
	 external_id
FROM {{source('public', 'raw_import_property_search_statuses')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('created','CETDST') }}  >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}