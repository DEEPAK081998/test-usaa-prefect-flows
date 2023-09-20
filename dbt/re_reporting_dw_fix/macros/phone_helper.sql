{% macro normalize_phone_number(column) %}
    -- Normalize phone number in 'column'
    CASE
        WHEN LENGTH({{ column }}) = 15
        AND
        {{ column }} LIKE('+%') THEN CONCAT('+', {{ regexp_replace_formatter(column, '\D', '') }})
        WHEN {{ column }}
        {{ not_null_operator() }}
        AND LENGTH(
            CONCAT('+1', {{ regexp_replace_formatter(column, '\D', '') }})
        ) >= 12 THEN LEFT(
            CONCAT('+1', {{ regexp_replace_formatter(column, '\D', '') }}),
            12
        )
        ELSE NULL
    END
{% endmacro %}
