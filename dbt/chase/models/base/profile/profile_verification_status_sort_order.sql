SELECT
    id,
    sort_order,
    status
FROM    
    {{ source('public', 'raw_profile_verification_status_sort_order') }}