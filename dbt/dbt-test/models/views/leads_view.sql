{{ config(materialized='view') }}

SELECT
id,
hash,
email,
phone,
bank_id,
CAST (created AS TIMESTAMP),
prequal,
CAST (updated AS TIMESTAMP),
comments,
last_name,
first_name,
aggregate_id,
rc_assignable,
sell_location,
contact_methods,
selling_address,
current_location,
transaction_type,
purchase_location,
brokerage_contacted,
purchase_time_frame,
price_range_lower_bound,
price_range_upper_bound,
purchase_location_detail,
referral_fee_transaction
FROM leads