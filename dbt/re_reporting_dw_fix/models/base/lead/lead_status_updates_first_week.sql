with lead_status_updates_first_week as (
select 
	row_number () over (partition by t1.id order by t2.created desc) row_id,
	t1.*,
	t2.status status_update_value,
	t2.created status_created
from {{ ref('leads_data_v3') }} t1 
	left join {{ ref('lead_status_updates') }} t2 
		on t1.id::VARCHAR = t2.lead_id::VARCHAR 
		{% if target.type == 'snowflake' %}
			and t2.created <= dateadd(day, 7, t1.reporting_enroll_date)
		{% else %}
			and t2.created <= (t1.reporting_enroll_date + interval '7 days')
		{% endif %}
)

select 
	*
from lead_status_updates_first_week 
where row_id = 1