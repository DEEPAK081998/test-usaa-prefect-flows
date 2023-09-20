{{
  config(
    materialized = 'incremental',
    unique_key = ['id','zip','state', 'county', 'st']
    )
}}
WITH

zips_in_county AS (
    SELECT county,st, count (distinct zip) as zipsincounty
    FROM {{ ref('cbsa_locations') }}
    GROUP BY county,st
)

,agent_county_coverage AS (
    SELECT zip,profile_id, count (distinct zip) as agentcountycoverage
    FROM {{ ref('profile_coverage_zips') }}
    GROUP BY profile_id,zip
)

,final_cte AS (
    SELECT 
        pup.id,
        pup.first_name as AgentFirstName,
        pup.last_name as AgentLastName,
        pup.eligibility_status,
        concat(pup.first_name,' ',pup.last_name) as AgentName,
        pup.email as AgentEmail,
        pup.phone as AgentPhone,
        pup.brokerage_code as BrokerageCode,
        pcz.zip,
        pcz.state,
        acc.agentcountycoverage,
        cbsa.county,
        cbsa.st,
        cz.zipsincounty,
        pup.updated as partner_updated_at
    FROM {{ ref('partner_user_profiles') }} pup
        JOIN {{ ref('profile_coverage_zips') }} pcz on pcz.profile_id = pup.id
        JOIN {{ ref('cbsa_locations') }} cbsa on pcz.zip = cbsa.zip
        LEFT OUTER JOIN zips_in_county cz on cz.county = cbsa.county and cz.st = cbsa.st
        LEFT OUTER JOIN agent_county_coverage acc on acc.profile_id = pup.id and acc.zip = pcz.zip
    {% if is_incremental() %}
    WHERE pup.updated >= coalesce((select max(partner_updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

SELECT distinct * FROM final_cte