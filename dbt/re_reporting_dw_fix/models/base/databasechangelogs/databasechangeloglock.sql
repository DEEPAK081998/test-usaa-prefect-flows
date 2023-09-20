{{ config(
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','databasechangeloglock_pkey') }}")
	]
) }}
select
	distinct
	 id,
	 locked,
	 cast(lockgranted as timestamp without time zone) as lockgranted_at,
	 lockedby
from {{source('public', 'raw_import_databasechangeloglock')}}
