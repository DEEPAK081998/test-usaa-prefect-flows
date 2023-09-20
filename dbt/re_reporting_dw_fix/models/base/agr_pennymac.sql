with source as (
      select * from {{ source('public'
, 'raw_pennymac') }}
),
renamed as (
    SELECT 
    agent_mobile_phone
    , customer_mobile_phone
    , customer_last_name
    , agent_email
    , customer_email
    , customer_first_name
    , agent_first_name
    , agent_last_name
    , customer_buy_or_sell_location
    FROM source
)
select * from renamed