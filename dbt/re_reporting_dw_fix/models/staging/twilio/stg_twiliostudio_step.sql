{{
  config(
    materialized = 'incremental',
    unique_key = 'sid')
}}

with twilio_step_filtered as(
	select
		row_number() over( partition by sid order by _airbyte_emitted_at) as row_id
		,sid
		,url
		,name
		,links
		,flow_sid
		,account_sid
		,date_created
		,date_updated
		,execution_sid
		,parent_step_sid
		,transitioned_to
		,transitioned_from
        ,_airbyte_emitted_at system_record_timestamp
	from {{ source('public', 'raw_twilliostudiostep') }}
	{% if is_incremental() %}
	where _airbyte_emitted_at >= coalesce((select max(system_record_timestamp) from {{ this }}), '1900-01-01')
	{% endif %})
select
	sid
	,url
	,name
	,links
	,flow_sid
	,account_sid
	,date_created
	,date_updated
	,execution_sid
	,parent_step_sid
	,transitioned_to
	,transitioned_from
	,system_record_timestamp
from twilio_step_filtered 
where row_id = 1	