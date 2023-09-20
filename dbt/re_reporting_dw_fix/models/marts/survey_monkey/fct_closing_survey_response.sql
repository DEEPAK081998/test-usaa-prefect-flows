{{
  config(
    materialized = 'table'
    )
}}
with cte as (
{% if target.type == 'snowflake' %}
    select cte.*,
	   custom_column.value:id::VARCHAR as page_id,
	   custom_column.value:questions as answers
	from {{ ref('stg_survey_response') }} cte,
	lateral flatten(input => cte.data_json) custom_column
{% else %}
    select cte.*,
	   custom_column.id as page_id,
	   custom_column.questions as answers
	from {{ ref('stg_survey_response') }} cte,pg_catalog.jsonb_to_recordset(cte.data_json)  as custom_column(id text, questions jsonb)
{% endif %}
)
, flatten_cte as (
{% if target.type == 'snowflake' %}
	select
	custom_column.value:id::VARCHAR as question_id,
	coalesce(custom_column.value:answers[0].text::VARCHAR, custom_column.value:answers[0].choice_metadata.weight::VARCHAR) as question_response,
	custom_column.value:answers[0].choice_id::VARCHAR as choice_id,
	int_cte.*
    from cte int_cte,
	lateral flatten(input => int_cte.answers) custom_column
{% else %}
	select
	custom_column.id as question_id,
	coalesce(custom_column.answers->0->>'text',custom_column.answers->0->'choice_metadata'->>'weight') as question_response,
	custom_column.answers->0->>'choice_id' as choice_id,
	int_cte.*
    from cte int_cte,pg_catalog.jsonb_to_recordset(int_cte.answers)  as custom_column(id text, answers jsonb)
{% endif %}
)
, choices_cte as (
{% if target.type == 'snowflake' %}
    select
    survey_questions.id as question_id,
    custom_column.value:id::VARCHAR as choice_id,
    custom_column.value:text::VARCHAR as choice_text
    from {{ source('public', 'survey_questions') }},
	lateral flatten(input => survey_questions.answers:choices) custom_column
{% else %}
    select  survey_questions.id as question_id,custom_column.id as choice_id,custom_column.text as choice_text from {{ source('public', 'survey_questions') }},pg_catalog.jsonb_to_recordset(survey_questions.answers->'choices')  custom_column(id text,text text)
{% endif %}
)
, int_cte as (
    select
    final_cte.lead_id,
    final_cte.customer_aggregate_id,
    l.agent_aggregate_id,
    final_cte.survey_response_id,
    final_cte.survey_id,
    final_cte.survey_subject,
    final_cte.survey_response_date,
    stg_cte.question_id,
    stg_cte.question_position,
    stg_cte.question_subject,
    COALESCE(cte.choice_text,final_cte.question_response) as question_response from flatten_cte final_cte
    left join {{ ref('stg_page_questions') }}  stg_cte
    on final_cte.question_id = stg_cte.question_id
    and final_cte.page_id = stg_cte.page_id
    left join {{ ref("leads_data_v3") }} l
    on final_cte.lead_id = l.id
    left join choices_cte cte
    on final_cte.question_id = cte.question_id
    and final_cte.choice_id = cte.choice_id
)
select * from int_cte