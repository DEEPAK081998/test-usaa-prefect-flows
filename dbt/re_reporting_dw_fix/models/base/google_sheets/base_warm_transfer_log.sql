SELECT 
    *
FROM
    {{ source('public', 'raw_warm_transfer_log') }}