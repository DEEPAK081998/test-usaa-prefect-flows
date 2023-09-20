-- Twilio filtered data for callduration
with callduration as (
	select 
		row_number () over(partition by lead_id order by event_timestamp desc) row_id,
		lead_id,
		callduration, 
		event_timestamp,
		{% if target.type == 'snowflake' %}
            DATEADD(day, 1, event_timestamp)  as event_timestamp_24h
        {% else %}
            (event_timestamp + interval '1 days') as event_timestamp_24h
        {% endif %}
	from {{ ref('stg_twilio_studio_journey_da') }}
	where callduration is not null and lead_id is not null)

-- Mapping data from leads status updates
, lead_status_edited as (
	select 
		lead_id, 
		created,
		{% if target.type == 'snowflake' %}
			CONVERT_TIMEZONE('America/Chicago', created) as created_cst,
		{% else %}
			created at time zone 'CST' as created_cst,
		{% endif %}
		status,
		case 
			when status like '%Actively%' 
				then 'Active' 
			else 
				case 
					when status like '%Inactive%' 
						then 'Inactive' 
				else null 
				end 
			end ext_status
	from {{ ref('lead_status_updates') }}
	where Category like 'Property%')

-- Merge twilio data with leads status updates when the last update is Active or Inactive
, callduration_lead as (
	select
		row_number () over(partition by ca.lead_id, ca.callduration, ca.event_timestamp, ca.row_id
						order by ca.event_timestamp desc, case when lse.created_cst is null then ca.event_timestamp else lse.created_cst end desc) row_id,
		ca.row_id as cd_row_num,
		ca.lead_id,
		ca.callduration,
		ca.event_timestamp,
		ca.event_timestamp_24h,
		lse.created_cst,
		lse.status,
		lse.ext_status	
	from callduration ca 
		left join lead_status_edited lse
			on ca.lead_id::VARCHAR = lse.lead_id::VARCHAR 
				and lse.ext_status is not null
				and ca.row_id = 1
				and (lse.created >= ca.event_timestamp and lse.created <= ca.event_timestamp_24h)
)

-- Merge twilio data with leads status updates when the last update is not equal to Active or Inactive
, callduration_lead_complete as (
	select 
		row_number () over(partition by ca.lead_id, ca.callduration, ca.event_timestamp, ca.cd_row_num
						order by ca.event_timestamp desc, case when lse.created_cst is null then ca.event_timestamp else lse.created_cst end desc) row_id,
		ca.lead_id,
		ca.callduration,
		ca.event_timestamp,
		case when ca.created_cst is null then lse.created_cst else ca.created_cst end as created_cst,
		case when ca.status is null then lse.status else ca.status end as status,
		case when ca.ext_status is null then case when lse.status is null then 'No Updates' else 'Other' end else ca.ext_status end as ext_status
	from callduration_lead ca 
		left join lead_status_edited lse
			on ca.lead_id::VARCHAR  = lse.lead_id::VARCHAR  
				and ca.ext_status is null
				and ca.cd_row_num = 1
				and (lse.created >= ca.event_timestamp and lse.created <= ca.event_timestamp_24h)
)

-- Query all data joined with lead_data_v3
select 
	lcd.lead_id,
	lcd.callduration,
	lcd.event_timestamp,
	lcd.created_cst,
	lcd.status,
	lcd.ext_status,
	ld.*
from {{ ref('leads_data_v3') }} ld
	left join callduration_lead_complete lcd
		on ld.id::VARCHAR  = lcd.lead_id::VARCHAR 
where (row_id = 1 or row_id is null)