WITH campaigns AS(
select 
	id,
	name,
	tags,
	type,
	state,
	active,
	actions,
	to_timestamp(created) AS campaign_created,
	to_timestamp(updated) AS campaign_updated,
    to_timestamp(first_started) AS campaign_first_started,
	timezone,
	frequency,
	event_name,
	start_hour,
	start_minutes,
	date_attribute,
	deduplicate_id,
	filter_segment_ids,
	trigger_segment_ids,
	use_customer_timezone
from
    {{ source('public', 'cio_prod_campaigns') }}
)
SELECT * FROM campaigns