WITH

brokerage_qualifications as (
    SELECT brokerage_code,qualification FROM {{ ref('brokerage_qualifications') }}
)

,profile_zipcount as (
    SELECT profile_id, count(*) as zip_count FROM {{ ref('profile_coverage_zips') }}
    GROUP BY profile_id
)

,final_cte as (
    SELECT 
        pup.id,
        pup.first_name as AgentFirstName,
        pup.last_name as AgentLastName,
        pup.eligibility_status,
        concat(pup.first_name,' ',pup.last_name) as AgentName,
        pup.email as AgentEmail,
        pup.phone as AgentPhone,
        pup.brokerage_code as BrokerageCode,
        case 
            when ZipCount.zip_count is null then 0
            else ZipCount.zip_count
        end as ZipCount,
        pup.updated as updated_at
    FROM {{ ref('partner_user_profiles') }} pup
    LEFT OUTER JOIN profile_zipcount ZipCount ON ZipCount.profile_id = pup.id
    LEFT OUTER JOIN brokerage_qualifications bq ON pup.brokerage_code = bq.brokerage_code
    {% if is_incremental() %}
    WHERE pup.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

SELECT * FROM final_cte
