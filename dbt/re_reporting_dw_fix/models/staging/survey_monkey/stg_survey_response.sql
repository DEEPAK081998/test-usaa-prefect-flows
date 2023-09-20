{{
  config(
    materialized = 'table'
    )
}}
select
    {% if target.type == 'snowflake' %}
    cast(sr.custom_variables:A::VARCHAR as int) as lead_id,
    sr.custom_variables:B::VARCHAR as customer_aggregate_id,
    {% else %}
    cast(sr.custom_variables->>'A' as int) as lead_id,
    cast(sr.custom_variables->>'B' as uuid) as customer_aggregate_id,
    {% endif %}
    sr.id as survey_response_id,
    sr.survey_id,
    s.title as survey_subject,
    sr.date_modified as survey_response_date,
    sr.pages as data_json
from {{ source('public', 'survey_responses') }} sr
left join {{ source('public', 'surveys') }} s
    on sr.survey_id = s.id