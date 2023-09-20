{%- macro dynamic_condition_table(table, json_conditions, versioned = false) -%}

{# /* 

Macro to create a reference table with conditions to me used in macro "dynamic_case_when".

* Parameters:
    1. table = table where the conditions are written;
    2. json_conditions = a json with the conditions to be written in the table. Example:
        {
            column_A: [
                {
                    "name": "Condition Test 1",
                    "result": "lead_id",
                    "condition": "major_status = 'closed'"
                },
                {
                    "name": "Condition Test 2",
                    "result": "lead_id",
                    "condition": "major_status = 'accepted'"
                }
            ]
        }
        This example json will load two lines in reference table. One with the name "Condition Test 1", the condition of 'closed' status and returning lead_id for each line that the condition is true, and the other with the name "Condition Test 2", the condition of 'accepted' status and returning lead_id for each line that the condition is true. the first key is the name of the column that will be created, in this case, "column_A"
    3. versioned = if there is the need of versioning the conditions, set this parameter to true. Default value = false
                               
*/ #}


{# /* if is a versioned condition table, check if it exists */ #}
{% if versioned %}

    {%- set table_exists = test_table_exists(table) -%}

{% else %}

    {%- set table_exists = false -%}

{% endif %}

{# /* creating the parsed json with the conditions */ #}
with parsed_json_cte as (
    select
        *
    from
        {% if target.type == 'snowflake' %}

            lateral flatten( input => parse_json( {{ json_conditions }} ))

        {% else %}

            jsonb_each({{ json_conditions }}::jsonb)

        {% endif %}              
),
{# /* creating the cte with conditions in table structure */ #}
new_condition_cte as (
    
    {% if target.type == 'snowflake' %}

    select
        t.key::text as column_name,
        f.value:name::text as condition_name,
        f.value:result::text as condition_result,
        f.value:condition::text as condition
    from
        parsed_json_cte t, lateral flatten (input => t.value) f

    {% else %}

    select
        key as column_name,
        jsonb_array_elements(value) ->> 'name'::text as condition_name,
        jsonb_array_elements(value) ->> 'result'::text as condition_result,
        jsonb_array_elements(value) ->> 'condition'::text as condition
    from
        parsed_json_cte

    {% endif %}
)
{# /* if is a versioned table and the table already exists, analyse if any condition changes */ #}
{% if table_exists %}
    ,
    old_condition_cte as (
        select
            column_name,
            condition_name,
            condition_result,
            condition,
            created_at,
            expiration_timestamp,
            active_condition
        from
            {{ table }}
    )
    select
        case 
            when new.condition_result is not null then new.column_name
            when new.condition_result is null then old.column_name
        end as column_name,
        case 
            when new.condition_result is not null then new.condition_name
            when new.condition_result is null then old.condition_name
        end as condition_name,
        case 
            when new.condition_result is not null then new.condition_result
            when new.condition_result is null then old.condition_result
        end as condition_result,
        case 
            when new.condition_result is not null then new.condition
            when new.condition_result is null then old.condition
        end as condition,
        case 
            when new.condition_result is not null then current_timestamp
            when new.condition_result is null then created_at
        end as created_at,
        case 
            when new.condition_result is not null then null::timestamptz
            when new.condition_result is null then current_timestamp
        end as expiration_timestamp,
        case 
            when new.condition_result is not null then 1
            when new.condition_result is null then 0
        end as active_condition
    from
        new_condition_cte new
        full outer join (
            select
                *
            from
                old_condition_cte
            where
                active_condition = 1) old 
                on new.condition_result = old.condition_result 
                    and new.condition_name = old.condition_name 
                    and new.condition = old.condition
                    and new.column_name = old.column_name 
    union
    select
        *
    from
        old_condition_cte
    where
        active_condition = 0

{% else %}

    select
        column_name,
        condition_name,
        condition_result,
        condition,
        current_timestamp as created_at,
        null::timestamptz as expiration_timestamp,
        1 as active_condition
    from
        new_condition_cte

{% endif %}

{%- endmacro -%}