WITH agent_coverage AS(
    select 
        pc.profile_id, 
        pc.zip,
        cbsa.city, 
        cbsa.county, 
        cbsa.st, 
        cbsa.cbmsa
  from {{ ref('profile_coverage_zips') }}  pc
  join {{ ref('cbsa_locations') }}  cbsa on pc.zip = cbsa.zip
)
SELECT  
    *
FROM agent_coverage ac
LEFT JOIN agent_profiles ap
ON ac.profile_id=ap.id