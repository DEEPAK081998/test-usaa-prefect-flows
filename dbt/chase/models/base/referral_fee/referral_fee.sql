{{ config(
    materialized = 'table',
) }}

WITH lead_fee AS (
    SELECT
        rca.id,
        rca.assignee_aggregate_id,
        rca.referral_fee_category_id,
        rfc.description,
        -- Add a ROW_NUMBER() window function to rank rows based on the updated date, partitioned by assignee_aggregate_id
        ROW_NUMBER() OVER (PARTITION BY rca.assignee_aggregate_id ORDER BY rca.updated DESC) AS rn
    FROM  {{ ref('referral_fee_category_assignments') }} rca
    LEFT JOIN {{ ref('referral_fee_categories') }} rfc
        ON rca.referral_fee_category_id = rfc.referral_fee_category_id
),
-- Filter the results to only keep rows with rn = 1 (i.e., the row with the maximum updated_date for each assignee_aggregate_id)

filtered_lead_fee AS (
    SELECT
        assignee_aggregate_id,
        referral_fee_category_id,
        description
    FROM lead_fee
    WHERE rn = 1
),

final_cte as(
select flf.*,
		percentage_fee
from filtered_lead_fee flf
left join referral_fee rf
on flf.assignee_aggregate_id = rf.assignee_aggregate_id
    and flf.referral_fee_category_id= rf.referral_fee_category_id 
order by assignee_aggregate_id desc
)
select * from final_cte