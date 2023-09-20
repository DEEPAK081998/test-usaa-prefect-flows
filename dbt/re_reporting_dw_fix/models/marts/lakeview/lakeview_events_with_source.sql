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
        'ga_eventlabel',
        'ga_eventaction',
        'ga_eventcategory',
        'ga_adcontent']
    )}} AS id,
    ga_date,
    ga_adcontent,
    view_id,
    ga_source,
    ga_eventlabel,
    ga_eventaction,
    ga_eventcategory,
    ga_totalevents,
    ga_uniqueevents,
    _airbyte_emitted_at AS updated_at
  from
    {{ source('public', 'lakeview_event_totals') }}
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
    ga_source,
    ga_eventlabel,
    ga_eventaction,
    ga_totalevents,
    ga_uniqueevents,
    ga_adcontent,
    updated_at

FROM {{ keyword_formatter('rows') }}
where rn = 1