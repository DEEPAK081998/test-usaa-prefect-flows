{{
  config(
    materialized = 'table'
    )
}}
select 
  {{ dbt_utils.star(from=source('public', 'raw_gs_agent_info'),
  except=['_AIRBYTE_RAW_GS_AGENT_INFO_HASHID',
          '_AIRBYTE_NORMALIZED_AT',
          '_AIRBYTE_EMITTED_AT',
          '_AIRBYTE_AB_ID'] ) }}
from 
	{{ source('public', 'raw_gs_agent_info') }} 
where agent_email is not null