{{
  config(
	materialized = 'table',
	tags=['lakeview','ga','goals']
	)
}}
select 
    TO_DATE(date, 'YYYYMMDD') AS formatted_date,
    sessionsourcemedium as sourcemedium,
    sessioncampaignname,
    SUM(CASE WHEN eventname = 'Saved Properties' THEN conversions ELSE 0 END) AS property_saved,
    SUM(CASE WHEN eventname = 'Saved Searches' THEN conversions ELSE 0 END) AS search_saved,
    SUM(CASE WHEN eventname = 'REC_Account_Created' THEN conversions ELSE 0 END) AS account_created,
    SUM(CASE WHEN eventname = 'Viewed_Search_Results_SRP' THEN conversions ELSE 0 END) AS viewed_search_results,
    SUM(CASE WHEN eventname = 'Viewed_Detail_Page_PDP' THEN conversions ELSE 0 END) AS viewed_details_page,
    SUM(CASE WHEN eventname = 'REC_Log_in' THEN conversions ELSE 0 END) AS logged_in,
    SUM(CASE WHEN eventname = 'Lead_submitted_New_Self_Enrollment' THEN conversions ELSE 0 END) as homestory_lead_submitted_new_self_enrollment,
    SUM(CASE WHEN eventname = 'All_Lead_Submited' THEN conversions ELSE 0 END) as homestory_lead_submitted,
    SUM(CASE WHEN eventname = 'Lead_submitted_PDP' THEN conversions ELSE 0 END) as homestory_lead_submitted_pdp
 from {{ source('public', 'ga4_lakeview_ga_goals_final_v2') }}  
group by date, sessionsourcemedium, sessioncampaignname

