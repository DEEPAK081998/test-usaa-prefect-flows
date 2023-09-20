{{
  config(
    materialized = 'table',
    enabled=false
    )
}}
SELECT * FROM ga_event_test
ORDER BY ga_date ASC