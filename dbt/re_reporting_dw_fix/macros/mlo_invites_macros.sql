{% macro reduce_mlo_invite_size() %}

  {#- gettin schema destination -#}
  {% set schema_dest = destination_schema() %}


  {% set create_tmp_table %}
  create table {{schema_dest}}.MLO_INVITES_DEDUP as ( 
    with CTE_ as (
    select *,ROW_NUMBER() OVER(partition by REPLACE(CAST (data->'invitationId' AS TEXT), '"','')  order by _AIRBYTE_EMITTED_AT ASC ) as ROW_ID from {{ source('public', 'dynamodb_mlo_invites') }} 
    )
    select "data", "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_dynamodb_mlo_invites_hashid", "role", email, nmlsid, loemail, lastname,
    sendgrid, firstname, shorthash, utmsource, disclosure, lolastname, lofirstname, phonenumber, utmcampaign, lophonenumber, mlosubmission, customerconnect, currentbankcustomer, confirmationrequired
    from CTE_ where ROW_ID= 1
    );
  {% endset %}
  {% set truncate_tbl %}
  truncate table {{ source('public', 'dynamodb_mlo_invites') }} ;
  {% endset %}
  {% set insert_dedup_records %}
  insert into {{ source('public', 'dynamodb_mlo_invites') }}  select * from {{schema_dest}}.MLO_INVITES_DEDUP ;
  {% endset %}
  {% set delete_tmp_tbl %}
  drop table {{schema_dest}}.MLO_INVITES_DEDUP ;
  {% endset %}




  {% do run_query(create_tmp_table) %}
  {% do run_query(truncate_tbl) %}
  {% do run_query(insert_dedup_records) %}
  {% do run_query(delete_tmp_tbl) %}

    
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