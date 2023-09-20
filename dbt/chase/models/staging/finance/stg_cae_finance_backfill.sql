WITH historic_cte AS (
select 
    cast(lead_id as bigint) as lead_id,
    _type_of_reward_,
    cast(nullif(replace(replace(replace(_rebate_owed_,'$',''),',',''),'-',''),' ') as decimal) as rebate_owed,
    _status_,
    replace(_rebate_paid_date_,'/','') as rebate_paid_date,
    _state_restrictions_,
    cast(nullif(replace(replace(replace(_referral_fee_expected_,'$',''),',',''),'-',''),' ') as decimal) as expected_fee,
    reviewer_1_approval
    
 from {{ source('public', 'historic_finance_data_aug2023') }}
where part_of_new_chase_program = 'Yes' and lead_id is not null 
)
select 
    lead_id,
    _type_of_reward_,
    rebate_owed,
    _status_,
    CASE
        WHEN length(rebate_paid_date) = 8 THEN to_date(rebate_paid_date, 'MMDDYYYY')
        WHEN length(rebate_paid_date) = 7 THEN to_date('0' || rebate_paid_date, 'MMDDYYYY')
        ELSE NULL
    END AS rebate_paid_date,
    case when _status_ ilike '%paid%' then 'true' else null end as rebate_paid_backfill,
    _state_restrictions_,
    expected_fee,
    reviewer_1_approval
FROM historic_cte