{{
  config(
	materialized = 'incremental',
	unique_key='id',
	tags=['pennymac','ga']
	)
}}

WITH dedup_inc AS
(

  select
    {{ dbt_utils.generate_surrogate_key([
        'ga_date',
        'view_id',
        'ga_pagepath']
    )}} AS id,
    ga_date,
    ga_pagepath,
    ga_users,
    ga_newusers,
    ga_sessions,
    ga_entrances,
    ga_pageviews,
    ga_bouncerate,
    ga_avgtimeonpage,
    ga_uniquepageviews,
    ga_avgsessionduration,
    ga_pageviewspersession,
    _airbyte_emitted_at AS updated_at
  from
    {{ source('public', 'pennymac_rep60_prod_ga_data') }}
  {% if is_incremental() %}
    where _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
  {% endif %}
  
),
{{ keyword_formatter('rows') }} AS(
select *,
    row_number() over(partition by id ORDER BY updated_at DESC) rn
 
from dedup_inc
)
SELECT  
    id,
    ga_date,
    ga_pagepath,
    ga_users,
    ga_newusers,
    ga_sessions,
    ga_entrances,
    ga_pageviews,
    ga_bouncerate,
    ga_avgtimeonpage,
    ga_uniquepageviews,
    ga_avgsessionduration,
    ga_pageviewspersession,
    updated_at

FROM {{ keyword_formatter('rows') }}
Where rn = 1

