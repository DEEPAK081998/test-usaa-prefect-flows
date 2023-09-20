{{
  config(
    materialized = 'table',
    unique_key = 'lead_id',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'}
	  ],
    tags=["notes"]
    )
}}

WITH 

final_cte AS (
{% if target.type == 'snowflake' %}
    SELECT DISTINCT lead_id, 1 as OutofNetwork,0 AS FailC, 0 AS FailD
    FROM {{ ref('note_updates') }}
    WHERE data:newContent::VARCHAR like '%OON%' and role = 'ADMIN'

    UNION ALL

    SELECT DISTINCT lead_id,0, 1,0
    FROM {{ ref('note_updates') }}
    WHERE data:newContent::VARCHAR like '%Fail: C%' and role = 'ADMIN'

    UNION ALL

    SELECT DISTINCT lead_id,0, 0,1
    FROM {{ ref('note_updates') }}
    WHERE data:newContent::VARCHAR like '%Fail: D%' and role = 'ADMIN'
{% else %}
    SELECT DISTINCT lead_id, 1 as OutofNetwork,0 AS FailC, 0 AS FailD
    FROM {{ ref('note_updates') }} 
    WHERE data->>'newContent' like '%OON%' and role = 'ADMIN'

    UNION ALL

    SELECT DISTINCT lead_id,0, 1,0
    FROM {{ ref('note_updates') }}
    WHERE data->>'newContent' like '%Fail: C%' and role = 'ADMIN'

    UNION ALL

    SELECT DISTINCT lead_id,0, 0,1
    FROM {{ ref('note_updates') }}
    WHERE data->>'newContent' like '%Fail: D%' and role = 'ADMIN'
{% endif %}
)
SELECT 
  lead_id,
  SUM(OutofNetwork) AS OutofNetwork,
  SUM(FailC) AS FailC,
  SUM(FailD) AS FailD 
FROM final_cte
GROUP BY lead_id