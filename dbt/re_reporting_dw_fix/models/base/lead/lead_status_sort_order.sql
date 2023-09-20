{{ config(
	materialized='incremental',
	unique_key='id',
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','lead_status_sort_order_pkey') }}")
	]
) }}
select
	 cast(id as bigint) as id,
	 sort_order,
	 status
from {{source('public', 'raw_import_lead_status_sort_order')}}
