{{
  config(
	materialized = 'table',
	tags=['bitly'],
  enabled=true
	)
}}
WITH lead_uuid_data AS(
SELECT 
    *
FROM 
    {{ ref('stg_cae_sms') }} scs
LEFT JOIN 
    {{ ref('base_bitly_sms') }} bbs ON scs.leaduuid = bbs.title),

lead_aggregateid_data AS(          
SELECT 
    *
FROM 
    {{ ref('stg_cae_sms') }} sc            
LEFT JOIN 
    base_bitly_sms bbs ON sc.lead_aggregateid = bbs.title),

union_data as(
select 
	*
from lead_uuid_data
union
select
	*
from lead_aggregateid_data 
)
select distinct * from union_data