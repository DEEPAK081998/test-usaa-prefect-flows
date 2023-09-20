{{
  config(
    materialized = 'table',
    )
}}
WITH
leads_data_cte AS (
    SELECT
        distinct on (ld.lead_id)
        ld.lead_id,
        json_array_elements(ld.documentation_obj->'statuses')->>'type' as rewardtype,
        json_array_elements(ld.documentation_obj->'statuses')->'user'->>'id' as senderID,
        json_array_elements(ld.documentation_obj->'statuses')->'user'->>'fullName' as senderName,
        (json_array_elements(ld.documentation_obj->'statuses')->>'createdAt')::bigint as rewardcreateddate,
        ld.documentation_obj->>'type' as rebatetype
    FROM {{ ref('stg_lead_data') }} ld
    WHERE documentation_obj is not null
    ORDER by ld.lead_id
)
SELECT * FROM leads_data_cte