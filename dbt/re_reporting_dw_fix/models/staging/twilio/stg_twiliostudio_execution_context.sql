{{
  config(
    materialized = 'incremental',
    unique_key = 'execution_sid')
}}

with twilio_execution_context_filtered as(
	select
		row_number() over( partition by execution_sid order by _airbyte_emitted_at) as row_id
	    ,url
		,context
		,flow_sid
		,account_sid
		,execution_sid
        ,_airbyte_emitted_at system_record_timestamp
	from {{ source('public', 'raw_twilliostudioexecution_context') }}
	{% if is_incremental() %}
	where _airbyte_emitted_at >= coalesce((select max(system_record_timestamp) from {{ this }}), '1900-01-01')
	{% endif %})
select 
	url
	,context
	,flow_sid
	,account_sid
	,execution_sid
	,system_record_timestamp
from twilio_execution_context_filtered 
where row_id = 1 