SELECT 
    *
FROM (
select
	*,
	row_number() over (partition by lead_id order by created DESC) rn
from 
{{ ref('note_updates') }}) t
where t.rn = 1