{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}
WITH dedup_inc AS
(

  select
    {{ dbt_utils.generate_surrogate_key([
        'ga_date',
        'view_id',
        'ga_source',
        'ga_campaign',
        'ga_pagepath',
        'ga_adcontent',
        'ga_dimension5']
    )}} AS id,
    ga_date,
    view_id,
    ga_source,
    ga_campaign,
    ga_pagepath,
    ga_adcontent,
    ga_dimension5,
    _airbyte_emitted_at AS updated_at,
    ga_users,
    ga_newusers,
    ga_sessions,
    ga_entrances,
    ga_pageviews,
    ga_bouncerate,
    ga_avgtimeonpage,
    ga_uniquepageviews,
    ga_avgsessionduration,
    ga_pageviewspersession
  from
    {{ source('public', 'freedom_custom_user_id') }}
  {% if is_incremental() %}
    where _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
  {% endif %}
  
),
{{ keyword_formatter('rows') }} AS(
select *,
    row_number() over(partition by id ORDER BY updated_at DESC) rn
 
from dedup_inc
)
SELECT * FROM {{ keyword_formatter('rows') }}
where rn = 1