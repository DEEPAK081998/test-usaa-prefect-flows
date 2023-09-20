SELECT
    id,
    date,
    client_name,
    client_email,
    client_phone,
    lo_name,
    lo_phone,
    lo_email,
    nmlsid,
    major_status,
    transaction_type
FROM
    {{ ref('leads_data_v3') }}
WHERE
    lo_name IS NULL
    AND bank_name LIKE 'PennyMac'
    AND major_status NOT ILIKE 'inactive'
    AND transaction_type LIKE 'BUY'
    AND {{ last_week_condition('date') }}
ORDER BY
    id
