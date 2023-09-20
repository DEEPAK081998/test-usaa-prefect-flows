{{
  config(
    materialized = 'table',
    )
}}
WITH
leads_data_cte AS (
{% if target.type == 'snowflake' %}
    SELECT
        ld.lead_id,
        fs.value:type::VARCHAR as rewardtype,
        fs.value:user.id::VARCHAR as senderID,
        fs.value:user.fullName::VARCHAR as senderName,
        (fs.value:createdAt::VARCHAR)::bigint as rewardcreateddate,
        ld.documentation_obj:type::VARCHAR as rebatetype
    FROM {{ ref('stg_lead_data') }} ld,
    lateral flatten(input => ld.documentation_obj:statuses) fs
    WHERE documentation_obj is not null
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ld.lead_id ORDER BY ld.lead_id) = 1
    ORDER by ld.lead_id
{% else %}
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
{% endif %}
)
SELECT * FROM leads_data_cte