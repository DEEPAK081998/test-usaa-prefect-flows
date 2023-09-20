{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['reminder_id'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','reminders_pkey') }}")
	]
) }}
select
	 cast(id as bigint) as id,
	 cast(reminder_id as {{ uuid_formatter() }}) as reminder_id,
	 email,
	 cast(lead_id as bigint) as lead_id,
	 reminder,
	 completion_status,
	 cast(due_date as timestamp without time zone) AS due_date,
	 {{ parse_json('data') }} AS data,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated
FROM {{source('public', 'raw_import_reminders')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
