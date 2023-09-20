{% macro snapshot_snapshot_agent_scorecard(type="I") %}

  {% set scorecard_version = 'v.1.0.0' %}

  {% set sql %}
   {% if  target.type == 'postgres' %}
      {% if type=='I' %}
        INSERT INTO {{target.database}}.{{target.schema}}.snapshot_agent_scorecard
        select date_trunc('day',now()) as snapshot_date,'{{ scorecard_version }}' as scorecard_version,* from {{ ref('fct_agent_scorecard') }}
      {% else %}
        DROP TABLE IF EXISTS {{target.database}}.{{target.schema}}.snapshot_agent_scorecard;
        CREATE TABLE {{target.database}}.{{target.schema}}.snapshot_agent_scorecard as (
        select date_trunc('day',now()) as snapshot_date,'{{ scorecard_version }}' as scorecard_version,* from {{ ref('fct_agent_scorecard') }}
        )
      {% endif %}
   {% endif %}
  {% endset %}
  {% do run_query(sql) %}

{% endmacro %}
