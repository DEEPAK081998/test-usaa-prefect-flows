SELECT
    id AS lead_id,
    {{ normalize_phone_number('client_phone')}} AS lead_phone_id,
    client_name,
    hb_status
FROM
    {{ ref('leads_data_v3') }}
WHERE hb_status NOT ILIKE '%inactive%'
