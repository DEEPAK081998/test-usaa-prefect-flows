with source as (
      select * from {{ source('public', 'raw_cio_verification_campaign_airbyte') }}
),
renamed as (
    select
         CAST(date AS date) AS date
         , lo_last_name
         , agent_email
         , normalizedcity
         , "Purchase Location" as purchase_location
         , agentfirstname
         , agent_phone
         , normalizedstate
         , enrollmenttype
         , lo_email
         , enrolleddate
         , client
         , TRIM(replace(customer_response_connected_with_agent,'\n','')) as customer_response_connected_with_agent
         , id
         , agentlastname
         , TRIM(replace(agent_respone_connected_with_customer,'\n','')) as agent_respone_connected_with_customer
         , client_phone
         , lo_first_name
         , client_first_name
         , client_email
         , agent_response_other_followup
         , normalizedzip
         , "Customer contacted?" as customer_contacted
         , client_last_name
         , lo_phone
         , customer_response_connected_with_lo

    from source
)
select * from renamed