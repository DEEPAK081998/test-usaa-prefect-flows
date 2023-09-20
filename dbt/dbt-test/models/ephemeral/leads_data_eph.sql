{{ config(materialized='ephemeral',sort='created',dist='id') }}
SELECT
id,
CAST (data as JSON),
CAST (created AS Timestamp),
lead_id,
CAST (updated AS Timestamp)
FROM leads_data