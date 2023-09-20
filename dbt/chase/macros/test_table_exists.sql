{%- macro test_table_exists(table) -%}

{# /* 

Macro to test if a table exists in the database
* Parameters:
      1. table = table to test. Has to be in the format database.schema.table
                               
*/ #}

{%- set source_relation = adapter.get_relation(
      database=table.database,
      schema=table.schema,
      identifier=table.name) -%}

{%- set table_exists=source_relation is not none -%}

{%- do return(table_exists) -%}

{%- endmacro -%}