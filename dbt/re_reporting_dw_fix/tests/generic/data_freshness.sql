{% test data_freshness(model, column_name, days) %}

    {{ config(severity = 'warn', store_failures = true) }}

    with max_cte as (
        select max({{ column_name }}) as latest_update
        from {{ model }}
    ),
    check_cte as (
    select latest_update
    from max_cte
    where NOW() - (SELECT latest_update FROM max_cte) > '{{ days }} day'::interval
    )
    select latest_update from check_cte
    where latest_update {{ not_null_operator() }}

{% endtest %}
