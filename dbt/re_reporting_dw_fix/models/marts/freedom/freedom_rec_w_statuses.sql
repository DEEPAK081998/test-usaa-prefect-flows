{{
  config(
    materialized = 'view',
  	tags=['freedom','rec']
    )
}}

WITH cte_base AS (
  SELECT 
      st.*,
      ROW_NUMBER() OVER (
        PARTITION BY st.id 
        ORDER BY st.created DESC
      ) AS row_id
  FROM {{ ref('stg_rec_w_statuses') }} st
  WHERE bank_id IN ('3405dc7c-e972-4bc4-a3da-cb07e822b7c6')
),
final_cte AS (
  SELECT 
    *,
  (
    CASE 
    WHEN LOWER(HB_Status) LIKE '%closed%' 
      THEN 'Closed'
    WHEN LOWER(HB_Status) LIKE 'active%' 
      THEN 'Active'
    WHEN LOWER(HB_Status) LIKE '%pending%' 
      THEN 'Pending'
    WHEN LOWER(HB_Status) LIKE 'inactive%' 
      THEN 'Inactive'
    ELSE 'Active' 
  END
  ) AS major_status
  FROM cte_base
)
,
previous_status_cte AS (
    select id,
    status as previous_status
    from final_cte
    where row_id = 2
)
SELECT 
f.*,
ps.previous_status
from final_cte f
left join previous_status_cte ps on ps.id = f.id