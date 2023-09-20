with source as (
      select * from {{ source('public', 'raw_inactive_campaign_airbyte') }}
),
renamed as (
    select
        {% if target.type == 'snowflake' %}
         TRY_TO_TIMESTAMP_NTZ(date, 'MM/DD/YY HH:MI AM') AS date
        {% else %}
        CAST(date AS timestamp) as date
        {% endif %}
        , lo_last_name
        , agent_email
        , normalizedcity
        , inactive_status
        , "Purchase Location" as purchase_location
        , agentfirstname
        , agent_phone
        , normalizedstate
        , enrollmenttype
        , lo_email
        , enrolleddate
        , client
        , id
        , agentlastname
        , client_phone
        , lo_first_name
        , client_first_name
        , client_email
        , TRIM(replace(customer_response,'\n','')) as customer_response
        , customer_notes
        , normalizedzip
        , "Customer contacted?" as customer_contacted
        , client_last_name
        , lo_phone
    from source
)
select * from renamed