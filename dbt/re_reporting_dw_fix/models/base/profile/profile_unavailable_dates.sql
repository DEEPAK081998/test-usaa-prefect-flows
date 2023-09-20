{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','profile_unavailable_dates_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(profile_id as bigint) as profile_id,
	 cast(date_start as timestamp without time zone) as date_start,
	 cast(date_end as timestamp without time zone) as date_end,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated
FROM {{source('public', 'raw_import_profile_unavailable_dates')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }}  >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}