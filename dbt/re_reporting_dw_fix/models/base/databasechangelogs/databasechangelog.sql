select
	 id,
	 author,
	 filename,
	 cast(dateexecuted as timestamp without time zone) as executed_at,
	 orderexecuted,
	 exectype,
	 md5sum,
	 description,
	 comments,
	 tag,
	 liquibase,
	 contexts,
	 labels,
	 deployment_id
from {{source('public', 'raw_import_databasechangelog')}}
