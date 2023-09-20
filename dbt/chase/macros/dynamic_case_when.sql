{%- macro dynamic_case_when(table, column_name, result_column, condition_column, valid_condition = true) -%}

{# /* 

Macro to create a case when clause in a dynamic way. It is necessary to exists a table with, at least, two columns:

1. A condition column where the conditions are write as a case when condition. Ex: column_A = 2 AND column_B > 0;
2. A result column where are the results for each condition. Ex: case when "condition" then "result".

* Parameters:
    1. table = table where the conditions are written;
    2. result_column = the column where the results are;
    3. condition_column = the column where the conditions are;
    4. valid_condition = Optional when the conditon table do not has version control. Necessary when the condition table has version control. 
                           This parameter indicates which condition/rule is usable. The same syntax in a where clause.
                           Ex: if the table has version control and exists a column named column_C with a flag equals to 1 when that condition has to be used
                               and 0 if not. Then the parameter has to contain the value "column_C = 1"
                               
*/ #}

{%- set result_query -%}

    SELECT 
        {{ result_column }} 
    FROM 
        {{ table }}
    WHERE
        {{ valid_condition }}
        AND lower(column_name) = lower('{{ column_name }}')
        AND lower({{ condition_column }}) != 'else'

{%- endset -%}

{%- set result = run_query(result_query) -%}

{%- set result_else_query -%}

    SELECT 
        {{ result_column }}  
    FROM 
        {{ table }}
    WHERE
        {{ valid_condition }}
        AND lower(column_name) = lower('{{ column_name }}')
        AND lower({{ condition_column }}) = 'else'

{%- endset -%}

{%- set result_else = run_query(result_else_query) -%}

{%- if execute -%}

    {%- set result_list = result.columns[0].values() -%}

    {% if result_else|length %}
        
        {%- set result_else = result_else -%}
    
    {% endif %} 

{%- else -%}

    {%- set result_list = [] -%}

{%- endif -%}

    case 

{% for result in result_list -%}

    {%- set condition_query -%}
        SELECT 
            {{ condition_column }}
        FROM 
            {{ table }}
        where
            {{ result_column }} = '{{ result }}'
            AND lower(column_name) = lower('{{ column_name }}')
            AND {{ valid_condition }}
    {%- endset -%}

    {%- set condition = run_query(condition_query) -%}

    {%- if execute -%}

    {%- set condition = condition.columns[0].values() -%}

    {%- else -%}

    {%- set condition = [] -%}

    {%- endif -%}

     when {{ condition[0] }} then '{{ result }}'

{% endfor %}

{% if result_else|length %}
    
    {%- set result_else = result_else.columns[0][0] -%}
    
    else '{{ result_else }}'
    
{% endif %}     
     
    end as {{ column_name }}

{%- endmacro -%}