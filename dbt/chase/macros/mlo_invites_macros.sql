{% macro reduce_mlo_invite_size() %}

  {#- gettin schema destination -#}
  {% set schema_dest = destination_schema() %}


  {% set get_current_version_sql %}
    select count(*) as current_version from pg_tables where tablename ilike 'dynamodb_mlo_invites%' and tablename ilike '%bkp%'
  {% endset %}

  {% set get_date_sql %}
    select date_trunc('month',(NOW() - INTERVAL '1' MONTH)) as date_to_clean
  {% endset %}

  {%- set get_current_version = dbt_utils.get_query_results_as_dict(get_current_version_sql) -%}
  {%- set get_date = dbt_utils.get_query_results_as_dict(get_date_sql) -%}

  {% set copy_data %}
    CREATE TABLE {{ schema_dest }}.dynamodb_mlo_invites_bkp{{ (get_current_version['current_version'][0] |int)+1 }} AS (
      SELECT * FROM {{ source('public', 'dynamodb_mlo_invites') }}
      WHERE "_airbyte_emitted_at" <= '{{ get_date["date_to_clean"][0] | string }}'
    )
  {% endset %}

  {% set create_index_sql %}
    CREATE INDEX ON {{ schema_dest }}.dynamodb_mlo_invites_bkp{{ (get_current_version['current_version'][0] |int)+1 }}  USING btree (_airbyte_emitted_at);
  {% endset %}

  {% set clean_data_sql %}
    DELETE FROM  {{ schema_dest }}.dynamodb_mlo_invites
    WHERE "_airbyte_emitted_at" <= '{{ get_date["date_to_clean"][0] | string }}'
  {% endset %}




  {% do run_query(copy_data) %}
  {% do run_query(create_index_sql) %}
  {% do run_query(clean_data_sql) %}

  {{ log('get_current_version: ' ~ get_current_version['current_version'][0], info=False) }}
  {{ log('copy_data: ' ~ copy_data, info=False) }}
  {{ log('create_index_sql: ' ~ create_index_sql, info=False) }}
  {{ log('clean_data_sql: ' ~ clean_data_sql, info=False) }}
    
{% endmacro %}


{% macro get_mlo_invite_bkp_tables() %}

  {% set schema_dest = destination_schema() %}

  {% set get_tables_stmnt %}
    select tablename as table_name from pg_tables where tablename ilike 'dynamodb_mlo_invites%' and schemaname ='{{ schema_dest }}' and tablename ilike '%bkp%';
  {% endset %}

  {%- set get_tables = dbt_utils.get_query_results_as_dict(get_tables_stmnt)['table_name'] -%}
  
  {{ log('get_tables: ' ~ get_tables, info=False) }}

  {{ return(get_tables) }}

{% endmacro %}