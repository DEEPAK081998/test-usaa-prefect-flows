with money_received_multi as (
select lead_id,
cast(json_array_elements(ld.data->'closingProcess'->'moneyReceivedMulti')->>'dateReceived' as bigint) as fee_date,
cast(nullif(replace(json_array_elements(ld.data->'closingProcess'->'moneyReceivedMulti')->>'amountReceived',',',''),'') as decimal) as amount,
json_array_elements(ld.data->'closingProcess'->'moneyReceivedMulti')->>'accountReceived' as bank_account,
json_array_elements(ld.data->'closingProcess'->'moneyReceivedMulti')->>'type' as referral_fee_type,
cast(ld.data->'closingProcess'->'moneyReceived'->>'amountReceived' as decimal) as referralamountreceived,
cast(nullif(replace(ld.data->'closingProcess'->>'expectedReferralFee',',',''),'') as decimal) as expected_fee
from {{ ref('leads_data') }} ld 
),
rownumber AS (
select m.lead_id,
to_timestamp(m.fee_date/1000) as fee_date,
m.amount,
case when m.referral_fee_type is null and m.amount <0 then 'adjustment'
when m.referral_fee_type is null and m.amount>=0 then 'deposit' 
else m.referral_fee_type end as referral_fee_type,
m.bank_account,
m.referralamountreceived,
m.expected_fee
from money_received_multi m
--where m.referral_fee_type is not null
order by lead_id desc, fee_date desc
)
select *,
ROW_NUMBER() over (PARTITION BY 
lead_id order by fee_date DESC) as row_id
from rownumber