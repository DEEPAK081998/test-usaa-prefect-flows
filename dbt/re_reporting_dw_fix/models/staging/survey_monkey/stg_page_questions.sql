{{
  config(
    materialized = 'table'
    )
}}
SELECT
    sq.page_id,
    sq.id AS question_id,
    sq.position AS question_position,
    {% if target.type == 'snowflake' %}
    sq.headings[0].heading::VARCHAR AS question_subject
    {% else %}
    sq.headings->0->>'heading' AS question_subject
    {% endif %}
FROM
    {{ source('public','survey_pages') }} sp
    LEFT JOIN {{ source('public','survey_questions') }} sq
    ON sq.page_id = sp.id
