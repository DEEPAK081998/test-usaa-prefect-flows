{{ config(materialized='view') }}
SELECT
id,
CAST (data as JSON),
CAST (created AS Timestamp),
lead_id,
CAST (updated AS Timestamp)
FROM leads_data