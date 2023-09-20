{% macro last_week_condition(date_column) %}
    -- Add conditions to filter last week data based on date column.
    ({{ date_column }} >= date_trunc('day', current_timestamp - INTERVAL '8 day')
    AND
    {{ date_column }}  <= date_trunc('day', current_timestamp - INTERVAL '1 day'))
{% endmacro %}

