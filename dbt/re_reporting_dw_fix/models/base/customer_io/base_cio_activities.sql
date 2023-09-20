
with activities AS(
{% if target.type == 'snowflake' %}
select
    id,
    --case when data->'opened' is not null then to_timestamp(cast(data->'opened' as int)) else null end as opened,
    case when data:opened='null' then null
        when (data:opened <> 'null' OR data is not null) then to_timestamp(cast(data:opened as int))
        end as opened,
    data:email."to"::VARCHAR as "to",
    data:email."from"::VARCHAR as "from",
    case when data:delivered='null' then null
        when (data:delivered <> 'null' OR data is not null) then to_timestamp(cast(data:delivered as int))
        end as delivered,
    case when (data:campaignId::VARCHAR <> '') then data:campaignId::VARCHAR else null end as campaign_id,
    data:delivery_id::VARCHAR as delivery_id,
    --data->>'campaignId' AS campaign_id,
    customer_identifiers:cio_id::VARCHAR as cio_id,
    data:reason::VARCHAR as reason,
    data:href::VARCHAR as href,
    data:campaign::VARCHAR as campaign,
    name,
    type,
    to_timestamp(timestamp) as timestamp,
    customer_id,
    delivery_type,
    customer_identifiers:email::VARCHAR as cust_email,
    customer_identifiers:cio_id::VARCHAR as cust_cio_id

{% else %}
select 
    id,
    --case when data->'opened' is not null then to_timestamp(cast(data->'opened' as int)) else null end as opened,
    case when data->'opened'='null' then null 
        when (data->'opened' <> 'null' OR data is not null) then to_timestamp(cast(data->'opened' as int)) 
        end as opened,
    data->'email'->>'to' as to,
    data->'email'->>'from' as from,
    case when data->'delivered'='null' then null 
        when (data->'delivered' <> 'null' OR data is not null) then to_timestamp(cast(data->'delivered' as int)) 
        end as delivered,
    case when (data->>'campaignId' <> '') then data->>'campaignId' else null end as campaign_id,
    data->>'delivery_id' as delivery_id,
    --data->>'campaignId' AS campaign_id,
    customer_identifiers->>'cio_id' as cio_id,
    data->>'reason' as reason,
    data->>'href' as href,
    data->>'campaign' as campaign,
    "name",
    type,
    to_timestamp(timestamp) as timestamp,
    customer_id,
    delivery_type,
    customer_identifiers->>'email' as cust_email,
    customer_identifiers->>'cio_id' as cust_cio_id
{% endif %}
from 
    {{ source('public', 'cio_prod_activities') }} 
)
SELECT * FROM activities
