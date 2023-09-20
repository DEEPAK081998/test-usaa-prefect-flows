SELECT
    date,
    id,
    client_first_name,
    client_last_name,
    client_email,
    client_phone,
    agent_name,
    agent_email,
    agent_phone,
    lo_name,
    lo_email,
    lo_phone,
    purchase_location,
    bankcustomer,
    transaction_type,
    hb_status,
    closedate,
    propertyaddress,
    propertyprice,
    lenderclosedwith,
    purchase_time_frame,
    avg_price
FROM
    {{ ref('leads_data_v3') }}
WHERE
    bank_name ILIKE 'rbc'
    AND closedate {{ not_null_operator() }}
ORDER BY
    closedate DESC