{{
  config(
	materialized = 'table',
	tags=['sofi','ga','goals']
	)
}}
select 
    TO_DATE(date, 'YYYYMMDD') AS formatted_date,
    sessionsourcemedium as sourcemedium,
    sessioncampaignname,
    SUM(CASE WHEN eventname = 'Saved Properties' THEN conversions ELSE 0 END) AS property_saved,
    SUM(CASE WHEN eventname = 'Saved Searches' THEN conversions ELSE 0 END) AS search_saved,
    SUM(CASE WHEN eventname = 'Viewed_Search_Results_SRP' THEN conversions ELSE 0 END) AS viewed_search_results,
    SUM(CASE WHEN eventname = 'Viewed_Detail_Page_PDP' THEN conversions ELSE 0 END) AS viewed_details_page,
    SUM(CASE WHEN eventname = 'REC_Log_in' THEN conversions ELSE 0 END) AS logged_in,
    SUM(CASE WHEN eventname = 'All_Lead_Submited' THEN conversions ELSE 0 END) as homestory_lead_submitted,
    SUM(CASE WHEN eventname = 'viewed_detail_page' THEN conversions ELSE 0 END) as viewed_detail_page,
    SUM(CASE WHEN eventname = 'Lead_submitted_Search_Landing_page' THEN conversions ELSE 0 END) as homestory_lead_submitted_search_landing_page,
    SUM(CASE WHEN eventname = 'Lead_submitted_About' THEN conversions ELSE 0 END) as homestory_lead_submitted_about_landing_page,
    SUM(CASE WHEN eventname = 'Lead_submitted_PDP' THEN conversions ELSE 0 END) as lead_submitted_pdp,
    SUM(CASE WHEN eventname = 'Lead_submitted_LO' THEN conversions ELSE 0 END) as homestory_lead_submitted_lo,
    SUM(CASE WHEN eventname = 'REC_Account_Created' THEN conversions ELSE 0 END) as account_created,
    SUM(CASE WHEN eventname = 'Viewed_Detail_Page_PDP' THEN conversions ELSE 0 END) as viewed_pdp, 
    SUM(CASE WHEN eventname = 'teambuilder_completion' THEN conversions ELSE 0 END) as teambuilder_completion,
    SUM(CASE WHEN eventname = 'am_i_purchase_ready_profile_builder' THEN conversions ELSE 0 END) as am_i_purchase_ready_profile_builder

 from {{ source('public', 'ga4_sofi_ga_goals_final_v2') }}  
group by date, sessionsourcemedium, sessioncampaignname



 


