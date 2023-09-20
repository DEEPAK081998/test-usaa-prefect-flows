{{
  config(
    materialized = 'view',
    )
}}
with cte as (
select
	lead_id,
	{% if target.type == 'snowflake' %}
	data:newContent::VARCHAR as attachment_code,
	{% else %}
	data->>'newContent' as attachment_code,
	{% endif %}
	created,
	row_number() over (partition by lead_id
order by
	created desc) as row_id
from
    {{ ref('note_updates') }}
where
	role = 'ADMIN'
	{% if target.type == 'snowflake' %}
	and data:changeType::VARCHAR <> 'DELETE'
	and data:newContent::VARCHAR like '%AR-%'
	{% else %}
	and data->>'changeType' <> 'DELETE'
	and data->>'newContent' like '%AR-%'
	{% endif %}
)
select
	lead_id,
	attachment_code,
	case
		when attachment_code = 'AR-CON' then 'New Construction Incentives'
		when attachment_code = 'AR-RES' then 'Lender Responsiveness/availability/Issue'
		when attachment_code = 'AR-CUS' then 'Customer Shopped Rates'
		when attachment_code = 'AR-PDL' then 'Partner Denied Loan'
		when attachment_code = 'AR-VVU' then 'Veteran went with Veterans United'
		when attachment_code = 'AR-PRE' then 'Pre existing relationship: Personal Bank/Local/Current lender'
		when attachment_code = 'AR-CPC' then 'Customer Paid Cash'
		when attachment_code = 'AR-ADC' then 'Agent Directed Customer'
		else null
	end as attachment_reason
from
	cte
where
	row_id = 1