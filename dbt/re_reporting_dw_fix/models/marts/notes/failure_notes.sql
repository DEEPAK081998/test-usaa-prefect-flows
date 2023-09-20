{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    indexes=[
	  {'columns': ['id'], 'type': 'hash'},
	  {'columns': ['created'], 'type': 'btree'},
	  ],
    )
}}

SELECT 
	nu.id,
	nu.lead_id,
	nu.created,
  {{ dbt_date.day_of_week('nu.created') }} AS day_of_week,
  (
    CASE 
      WHEN {{ dbt_date.day_of_week('nu.created') }} >= 6 THEN TRUE
      ELSE FALSE
    END 
  ) AS is_weekend,
  {% if target.type == 'snowflake' %}
      nu.data:changeType::VARCHAR AS change_type,
      (
        CASE
          WHEN nu.data:newContent::VARCHAR LIKE '%Fail: C%' AND nu.role = 'ADMIN' THEN 'Fail: C'
          WHEN nu.data:newContent::VARCHAR LIKE '%Fail: D%' AND nu.role = 'ADMIN' THEN 'Fail: D'
          ELSE NULL
        END
      ) AS failure_type
    FROM 
      {{ ref('note_updates') }} nu
    WHERE 
      (nu.data:newContent::VARCHAR LIKE '%Fail: C%' AND nu.role = 'ADMIN')
      OR (nu.data:newContent::VARCHAR LIKE '%Fail: D%' AND nu.role = 'ADMIN')
      {% if is_incremental() %}
        AND created >= COALESCE((SELECT MAX(created) FROM {{ this }}), '1900-01-01')
      {% endif %}
  {% else %}
      nu.data->>'changeType' as change_type,
      (
        CASE
          WHEN nu.data->>'newContent' LIKE '%Fail: C%' AND nu.role = 'ADMIN' THEN 'Fail: C'
          WHEN nu.data->>'newContent' LIKE '%Fail: D%' AND nu.role = 'ADMIN' THEN 'Fail: D'
          ELSE NULL
        END
      ) AS failure_type
    FROM 
      {{ ref('note_updates') }} nu
    WHERE 
      (nu.data->>'newContent' LIKE '%Fail: C%' AND nu.role = 'ADMIN')
      OR (nu.data->>'newContent' LIKE '%Fail: D%' AND nu.role = 'ADMIN')
      {% if is_incremental() %}
        AND created >= COALESCE((SELECT MAX(created) FROM {{ this }}), '1900-01-01')
      {% endif %}
  {% endif %}