{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
	],
    )
}}
  SELECT 
    distinct on (lsu.lead_id) lsu.lead_id,
    lsu.data::json->>'salePrice' as HomesalePrice
  FROM {{ ref('lead_status_updates') }}  lsu
  JOIN {{ ref('current_lead_statuses') }}  cls on cls.lead_id = lsu.lead_id
  WHERE lsu.data::json->'salePrice' is not null  and cls.status like '%Closed%'
  GROUP BY lsu.data::json->>'salePrice', lsu.lead_id