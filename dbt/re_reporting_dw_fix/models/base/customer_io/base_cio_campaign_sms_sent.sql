{{
  config(
    materialized = 'table'
    )
}}
with cte as ( 
	select *,
	{% if target.type == 'snowflake' %}
	TO_TIMESTAMP_NTZ(cpad.timestamp) AS ts_ntz,
    CONVERT_TIMEZONE('UTC', ts_ntz) AS system_date,
    CONVERT_TIMEZONE('America/Chicago', ts_ntz) AS reporting_date
	{% else %}
    to_timestamp(cpad.timestamp) at time zone 'UTC' as system_date,
    to_timestamp(cpad.timestamp) at time zone 'CST' as reporting_date 
	{% endif %}
  from {{ source('public', 'cio_prod_activities') }} cpad  WHERE cpad.delivery_type ='webhook' and (cpad.name is null or cpad.name <> 'outreach')
	and delivery_id  in ( select delivery_id from {{ source('public', 'cio_prod_activities') }}  WHERE delivery_type ='webhook' and (name is null or name <> 'outreach') and type = 'sent_action')
)
select cte.delivery_id,cte.type,ciopm.subject,cte.data,
CASE
			WHEN ciopm.campaign_id  IN (23,24,14,21,20,11,18,26,2,25,44) THEN 'Verified to Pending'
			WHEN ciopm.campaign_id IN (39,31,37,36,42) THEN 'Close to Reward'
			WHEN ciopm.campaign_id = 44 THEN 'Enroll to Verify'
			ELSE NULL
		END AS customer_journey,
		cte.customer_identifiers,
		cte.delivery_type,
		cte.name,
		cte.timestamp,
		cte.system_date,
		cte.reporting_date,
		ciopm.recipient,
		ciopm.metrics,
		ciopm.customer_id,
		ciopm.campaign_id 
		from cte 
join {{ source('public', 'cio_prod_messages') }} ciopm on ciopm.id = cte.delivery_id
