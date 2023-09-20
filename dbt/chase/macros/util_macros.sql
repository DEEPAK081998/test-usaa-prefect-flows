{#- generate_schema_name: generate the custom schema name  -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set target_name = target.name|upper -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {% if env_var("DBT_RUNS_LOCALLY","FALSE")|upper=="FALSE" %}
          {{ env_var(target_name ~ '_SCHEMA',default_schema) }}
        {% else %}
          {{ env_var("DBT_USER","develop") }}_public
        {% endif %}
    {% endif %}
{%- endmacro %}


{% macro destination_schema() %}

  {% if env_var("DBT_RUNS_LOCALLY","FALSE")|upper=="FALSE" %}
      {% set schema_dest = env_var(target_name ~ '_SCHEMA',target.schema) %}
  {% else %}
      {% set schema_dest = env_var("DBT_USER","develop")~"_public" %}
  {% endif %}

  {{ return(schema_dest) }}

{% endmacro %}


{#- add_primary_key: Return query to send it on a post hook only when full refresh happen -#}
{% macro add_primary_key(model,column,primary_key_name) -%}
    {{ return(adapter.dispatch('add_primary_key')(model,column,primary_key_name)) }}
{%- endmacro %}

{% macro postgres__add_primary_key(model,column,primary_key_name) -%}
    {% set sql %}
       ALTER TABLE ONLY {{ model }} ADD CONSTRAINT {{ primary_key_name }} PRIMARY KEY ({{ column }})
    {% endset %}
    {% if not is_incremental() %}
        {{ sql }}
    {% else %}
        {{ "" }}
    {% endif %}
{%- endmacro %}

{% macro snowflake__add_primary_key(model,column,primary_key_name) %}
    {% do return(None) %}
{% endmacro %}

{#- convert_timezone: Convert the date field to epoch and then to the timezone required -#}
{% macro convert_timezone(column,timezone) -%}
    {{ return(adapter.dispatch('convert_timezone')(column,timezone)) }}
{%- endmacro %}

{% macro postgres__convert_timezone(column,timezone) -%}
    timezone('{{ timezone }}', cast({{ column }} as timestamp without time zone ))
{%- endmacro %}

{% macro snowflake__convert_timezone(column,timezone) %}
    CONVERT_TIMEZONE('{{ timezone }}',{{ column }})
{% endmacro %}

{#- uuid_formatter: Returns correct data type for uuid -#}
{% macro uuid_formatter() -%}
    {{ return(adapter.dispatch('uuid_formatter')()) }}
{%- endmacro %}

{% macro postgres__uuid_formatter() -%}
    uuid
{%- endmacro %}

{% macro snowflake__uuid_formatter() %}
    VARCHAR
{% endmacro %}

{#- special_column_name_formatter: Add "" when special name is used as column name -#}
{% macro special_column_name_formatter(name) -%}
    {{ return(adapter.dispatch('special_column_name_formatter')(name)) }}
{%- endmacro %}

{% macro postgres__special_column_name_formatter(name) -%}
    "{{ name }}"
{%- endmacro %}

{% macro snowflake__special_column_name_formatter(name) %}
        {{ name }}
{% endmacro %}

{#- date_diff_columns: get date diff between two fields -#}
{% macro date_diff_columns(date_part,initial_date,end_date=None) -%}
    {{ return(adapter.dispatch('date_diff_columns')(date_part,initial_date,end_date)) }}
{%- endmacro %}


{% macro postgres__date_diff_columns(date_part,initial_date,end_date=None) -%}
    {%- if end_date -%}
        extract({{date_part}} from ({{end_date}} - {{initial_date}}))::int
    {%- else -%}
        extract({{date_part}} from (current_date - {{initial_date}}))::int
    {%- endif -%}
{%- endmacro %}

{% macro snowflake__date_diff_columns(date_part,initial_date,end_date=None) %}
    {%- if end_date -%}
        datediff('{{ date_part }}', {{initial_date}}, {{end_date }})
    {%- else -%}
        datediff('{{ date_part }}', {{initial_date}}, current_date())
    {%- endif -%}
{% endmacro %}