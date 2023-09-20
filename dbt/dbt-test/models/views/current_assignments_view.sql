{{ config(materialized='view') }}
SELECT
id,
role,
email,
lead_id,
CAST(profile AS JSON),
CAST(partner_id AS UUID),
CAST(profile_aggregate_id AS UUID)
FROM current_assignments