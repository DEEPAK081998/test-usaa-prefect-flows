{{
  config(
    materialized = 'table',
	enabled=false
    )
}}
select 
    *
from {{ ref('pmc_rec_w_statuses') }}
UNION
select 
    *
from {{ ref('pmc_rec_w_statuses_agent_submitted') }}