{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}
WITH dedup_inc AS
(

  select DISTINCT
    {{ dbt_utils.generate_surrogate_key(
        ['ga_date',
        'ga_source',
        'ga_campaign',
        'ga_medium',
        'ga_adcontent']
    )}} AS id,
    ga_date,
    ga_source,
    ga_campaign,
    ga_medium,
    ga_adcontent,
    ga_goal1completions AS enrollments,
    {{ current_date_time() }} AS updated_at
    from
      {{ source('public', 'lakeview_sg_conversions') }}
    {% if is_incremental() %}
    where _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
),

{{ keyword_formatter('rows') }} AS(
    select 
        *,
        row_number() over(partition by id ORDER BY updated_at DESC) rn
 
    from dedup_inc
)

SELECT * FROM {{ keyword_formatter('rows') }}
where rn = 1