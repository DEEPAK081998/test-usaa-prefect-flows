{#- generate_schema_name: generate the custom schema name  -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set target_name = target.name|upper -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {% if env_var("DBT_RUNS_LOCALLY","FALSE")|upper=="FALSE" %}
            {% if custom_schema_name =='test_failures' %}
              {{custom_schema_name}}
            {% else %}
               {{ env_var(target_name ~ '_SCHEMA',default_schema) }}
            {% endif %}
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
{% macro local_convert_timezone(column,timezone='CETDST') -%}
    {{ return(adapter.dispatch('local_convert_timezone')(column,timezone)) }}
{%- endmacro %}

{% macro postgres__local_convert_timezone(column,timezone='CETDST') -%}

{% if timezone=='CETDST' %}
  CASE
    WHEN EXTRACT(MONTH FROM {{ column}}) BETWEEN 3 AND 10
    AND EXTRACT(ISODOW FROM {{column}}) = 0 -- Sunday
    AND EXTRACT(DAY FROM {{column}}) >= 31 - (7 - DATE_PART('ISODOW', DATE_TRUNC('MONTH', {{column}})::DATE))
    THEN timezone('CEST', cast({{ column }} as timestamp without time zone ))
    ELSE timezone('CET', cast({{ column }} as timestamp without time zone )) 
  END
{% else %}
 timezone('{{timezone}}', cast({{ column }} as timestamp without time zone )) 
{% endif %}

    
{%- endmacro %}

{% macro snowflake__local_convert_timezone(column,timezone='CET') %}
    {% if timezone == 'CETDST' %}
        CONVERT_TIMEZONE('CET', 'America/Chicago',{{ column }})
    {% elif timezone == 'CST' %}
        CONVERT_TIMEZONE('America/Chicago','America/Chicago', {{ column }})
    {% else %}
    CONVERT_TIMEZONE('{{ timezone }}','America/Chicago', {{ column }})
    {% endif %}
{% endmacro %}


{#- keyword_formatter: Add "" when keyword is used as name -#}
{% macro keyword_formatter(name) -%}
    {{ return(adapter.dispatch('keyword_formatter')(name)) }}
{%- endmacro %}

{% macro postgres__keyword_formatter(name) -%}
    {{ name }}
{%- endmacro %}

{% macro snowflake__keyword_formatter(name) %}
       "{{ name }}"
{% endmacro %}


{#- current_date_time: Returns current date and time -#}
{% macro current_date_time() -%}
    {{ return(adapter.dispatch('current_date_time')()) }}
{%- endmacro %}

{% macro postgres__current_date_time() -%}
    NOW()
{%- endmacro %}

{% macro snowflake__current_date_time() %}
    CURRENT_TIMESTAMP()
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


{#- parse_json: Transforms json data to correct data type -#}
{% macro parse_json(column) -%}
    {{ return(adapter.dispatch('parse_json')(column)) }}
{%- endmacro %}

{% macro postgres__parse_json(column) -%}
    cast({{ column }} as json)
{%- endmacro %}

{% macro snowflake__parse_json(column) %}
    PARSE_JSON({{ column }})
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


{#- not_null_operator: Format is not null operator per target -#}
{% macro not_null_operator() -%}
    {{ return(adapter.dispatch('not_null_operator')()) }}
{%- endmacro %}

{% macro postgres__not_null_operator() -%}
    notnull
{%- endmacro %}

{% macro snowflake__not_null_operator() %}
    is not null
{% endmacro %}


{#- is_null_operator: Format is null operator per target -#}
{% macro is_null_operator() -%}
    {{ return(adapter.dispatch('is_null_operator')()) }}
{%- endmacro %}

{% macro postgres__is_null_operator() -%}
    isnull
{%- endmacro %}

{% macro snowflake__is_null_operator() %}
    is null
{% endmacro %}


{#- regexp_replace_formatter: Formats regexp_replace function for specific target when all occurrences are replaced -#}
{% macro regexp_replace_formatter(subject, pattern, replacement='') -%}
    {{ return(adapter.dispatch('regexp_replace_formatter')(subject, pattern, replacement='')) }}
{%- endmacro %}

{% macro postgres__regexp_replace_formatter(subject, pattern, replacement='') -%}
    REGEXP_REPLACE({{ subject }}, '{{ pattern }}', '{{ replacement }}', 'g')
{%- endmacro %}

{% macro snowflake__regexp_replace_formatter(subject, pattern, replacement='') %}
   REGEXP_REPLACE({{ subject }}, '{{ pattern }}', '{{ replacement }}', '1', '0')
{% endmacro %}


{#- calculate_time_interval: Adds or subtracts time interval -#}
{% macro calculate_time_interval(subject, sign, amount, time_unit) -%}
    {{ return(adapter.dispatch('calculate_time_interval')(subject, sign, amount, time_unit)) }}
{%- endmacro %}

{% macro postgres__calculate_time_interval(subject, sign, amount, time_unit) -%}
    ({{ subject }} {{ sign }} interval '{{ amount }}' {{ time_unit }})
{%- endmacro %}

{% macro snowflake__calculate_time_interval(subject, sign, amount, time_unit) %}
    DATEADD({{ time_unit }}, {{ sign }}{{ amount }}, {{ subject }})
{% endmacro %}

{% macro extract_email_prefix(column) %}
    {#- Return email prefix by keep everything before the @ symbol.-#}
    SPLIT_PART(lower({{ column }}), '@', 1)
{% endmacro %}

{#- filter_previous_day: Filter from midnight from previous day till midnight from current day on a selected timezone -#}
{% macro filter_previous_day(column, timezone) -%}
    {{ return(adapter.dispatch('filter_previous_day')(column, timezone)) }}
{%- endmacro %}

{% macro postgres__filter_previous_day(column, timezone) -%}
    {{ column }} BETWEEN (DATE(current_timestamp AT TIME ZONE '{{ timezone }}')-1)::timestamp
    AND (DATE(current_timestamp AT TIME ZONE '{{ timezone }}'))::timestamp - INTERVAL '1 ms'
{%- endmacro %}

{% macro snowflake__filter_previous_day(column, timezone) %}
    {{ column }} BETWEEN (TO_DATE(CONVERT_TIMEZONE('{{ timezone }}', CURRENT_TIMESTAMP()))-1)::timestamp
    AND (TO_DATE(CONVERT_TIMEZONE('{{ timezone }}', CURRENT_TIMESTAMP())))::timestamp - INTERVAL '1 ms'
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
