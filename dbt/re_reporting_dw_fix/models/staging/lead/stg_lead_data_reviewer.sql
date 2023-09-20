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
        ld.rebate_obj:received.user.id::VARCHAR as reviewer1id,
        ld.rebate_obj:received.user.fullName::VARCHAR as finalreviewname,
        (ld.rebate_obj:received.checkedAt::VARCHAR)::bigint as finalreviewdate,
        ld.rebate_obj:reviewers[0].user.id::VARCHAR as finalreviewid,
        ld.rebate_obj:reviewers[0].user.fullName::VARCHAR as reviewer1name,
        (ld.rebate_obj:reviewers[0].checkedAt::VARCHAR)::bigint as reviewer1date,
        ld.rebate_obj:reviewers[1].user.id::VARCHAR as reviewer2id,
        ld.rebate_obj:reviewers[1].user.fullName::VARCHAR as reviewer2name,
        (ld.rebate_obj:reviewers[1].checkedAt::VARCHAR)::bigint as reviewer2date,
        ld.updated_at
    FROM {{ ref('stg_lead_data') }} ld
    WHERE ld.rebate_obj is not null
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ld.lead_id ORDER BY ld.lead_id) = 1
    ORDER BY ld.lead_id
{% else %}
    SELECT 
        distinct on (ld.lead_id)
        ld.lead_id,
        ld.rebate_obj->'received'->'user'->>'id' as reviewer1id,
        ld.rebate_obj->'received'->'user'->>'fullName' as finalreviewname,
        (ld.rebate_obj->'received'->>'checkedAt')::bigint as finalreviewdate,
        ld.rebate_obj->'reviewers'->0->'user'->>'id' as finalreviewid,
        ld.rebate_obj->'reviewers'->0->'user'->>'fullName' as reviewer1name,
        (ld.rebate_obj->'reviewers'->0->>'checkedAt')::bigint as reviewer1date,
        ld.rebate_obj->'reviewers'->1->'user'->>'id' as reviewer2id,
        ld.rebate_obj->'reviewers'->1->'user'->>'fullName' as reviewer2name,
        (ld.rebate_obj->'reviewers'->1->>'checkedAt')::bigint as reviewer2date,
        ld.updated_at
    FROM {{ ref('stg_lead_data') }} ld
    WHERE ld.rebate_obj is not null
    ORDER BY ld.lead_id
{% endif %}
)
SELECT * FROM leads_data_cte