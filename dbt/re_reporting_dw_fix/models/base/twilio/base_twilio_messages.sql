SELECT
    *,
    {{ current_date_time() }} AS _updated_at
FROM
    {{ source('public', 'raw_hs_messages') }}