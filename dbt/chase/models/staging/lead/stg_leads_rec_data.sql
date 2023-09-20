{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}
SELECT 
    leads.id,
    leads.created,
    leads.updated,
    leads.first_name,
	  leads.last_name,
	  leads.email,
	  replace(phone,'+','') as client_phone,
    purchase_location,
    sell_location,
    current_location,
    case 
      when purchase_time_frame = 1 then 90
      when purchase_time_frame = 2 then 180 else 365 end as purchase_time_frame,
    prequal,
    (
      case 
          when leads.price_range_lower_bound is null or leads.price_range_lower_bound = 0
              then leads.price_range_upper_bound::decimal 
          when leads.price_range_upper_bound is null or leads.price_range_lower_bound = 0
              then leads.price_range_lower_bound::decimal 
          else ((leads.price_range_lower_bound::decimal +leads.price_range_upper_bound::decimal)/2) 
          end
    ) as avg_price,
    case when transaction_type = 'PURCHASE' then 'BUY'
    when transaction_type = 'BOTH' then 'BUY' 
    else transaction_type end as transaction_type,
    price_range_lower_bound,
    price_range_upper_bound,
    case when transaction_type = 'SELL' then nl.normalized_sell_location::json->>'zip' else nl.normalized_purchase_location::json->>'zip' end as zip,
    case when transaction_type = 'SELL' then nl.normalized_sell_location::json->>'city' else nl.normalized_purchase_location::json->>'city' end as city,
    case when transaction_type = 'SELL' then nl.normalized_sell_location::json->>'state' else nl.normalized_purchase_location::json->>'state' end as state
FROM
{{ ref('leads') }} 
join {{ ref('normalized_lead_locations') }}  nl on nl.lead_id = leads.id
{% if is_incremental() %}
WHERE leads.updated >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}