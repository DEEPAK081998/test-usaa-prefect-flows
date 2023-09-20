{{
  config(
    materialized = 'incremental',
    unique_key = 'step_sid')
}}

with twilio_step_context_filtered as(
	select
		row_number() over( partition by step_sid order by _airbyte_emitted_at) as row_id
		,url
		,context
		,flow_sid
		,step_sid
		,account_sid
		,execution_sid
        ,_airbyte_emitted_at system_record_timestamp
	from {{ source('public', 'raw_twilliostudiostep_context') }}
	{% if is_incremental() %}
	where _airbyte_emitted_at >= coalesce((select max(system_record_timestamp) from {{ this }}), '1900-01-01')
	{% endif %})
select 
	url
	,context
	,flow_sid
	,step_sid
	,account_sid
	,execution_sid
	,system_record_timestamp
from twilio_step_context_filtered 
where row_id = 1