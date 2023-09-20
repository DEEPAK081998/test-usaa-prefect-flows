select 
    state,
    transaction_type,
    state_restriction
    from {{ source('public', 'state_restrictions_finance') }}