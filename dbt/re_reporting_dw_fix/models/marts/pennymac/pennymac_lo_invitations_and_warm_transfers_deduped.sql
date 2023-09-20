With
cte AS (
        SELECT 
                1 as activity_count,
                'Warm Transfer' as activity_type,
                date(wt.date) as activity_date,
                wt.leadid::bigint as leadId,
                wt.nmlsid as inviting_nmlsid,
                wt.loan_officer_name as inviting_lo,
                NULL AS lo_aggregate_id,
                '' as transaction_type,
                wt.lo_contact_attempted,
                wt.partner as partner,
                wt.declined, wt.enrolled,
                wt.decline_reason,
                wt.client_name as invitee_name,
                date({{ calculate_time_interval('ld.created', '-', '7', 'hour') }}) as enrolled_date,
                ld.nmlsid as enrollment_lo,
                ld.client_name,
                ld.client_email,
                ld.client_phone,
                ld.agent_name,
                ld.agent_email,
                ld.agent_phone,
                ld.purchase_location,
                ld.purchase_time_frame,
                ld.prequal,
                ld.price_range_lower_bound,
                ld.price_range_upper_bound,
                ld.bank_name,
                '0' as total_opens,
                '0' as total_clicks,
                '0' as email_sent,
                CAST('0' AS VARCHAR(10)) as unique_campaign_open,
                CAST('0' AS VARCHAR(10)) as unique_campaign_click,
                '' as invite_id,
                '' as invite_email,
                '' as lofirstname,
                '' as lolastname,
                '' as agent,
                '' as purchase_specialist,
                '' as mlo_submission,
                '' as sms_invite_permission,
                '' as role,
                '' as loemail,
                '' as lastname,
                '' as firstname,
                '' as sendgrid,
                '' as shorthash,
                '' as utmsource,
                '' as disclosure,
                '' as phonenumber,
                '' as utmcampaign,
                '' as lophonenumber,
                '' as customerconnect,
                '' as confirmationrequired,
                ld.closedate,
                ld.lsuclosedate,
                ld.state,
                ld.city,
                ld.zip,
                1 as row_id
        FROM {{ ref('warm_transferlog') }} wt
        LEFT OUTER JOIN {{ ref('leads_data_v3') }} ld on wt.leadid::int = ld.id::int
        WHERE wt.date is not null and partner = 'PennyMac'

        UNION

        SELECT
                1 as activity_count,
                'Email' as activity_type,
                date(si.invite_date) as activity_date,
                t1.id as leadId, 
                si.nmlsid as inviting_nmlsid,
                concat(si.lofirstname,' ', si.lolastname) as inviting_lo,
                pup.aggregate_id AS lo_aggregate_id,
                t1.transaction_type,
                'NA' as lo_contact_attempted,
                case when t1.bank_name is null then 'PennyMac' else t1.bank_name end as partner, 'NA' as declined, 
                case when t1.id is null then 'FALSE' else 'TRUE' end as enrolled,
                'NA' as declineReason, concat( si.firstname, ' ',si.lastname) as ClientName,
                date({{ calculate_time_interval('t1.created', '-', '7', 'hour') }}) as enrolled_date,
                t1.nmlsid as enrollment_lo, 
                t1.client_name, 
                t1.client_email, 
                t1.client_phone, 
                t1.agent_name, 
                t1.agent_email, 
                t1.agent_phone,
                t1.purchase_location, 
                t1.purchase_time_frame, 
                t1.prequal, 
                t1.price_range_lower_bound, 
                t1.price_range_upper_bound, 
                t1.bank_name,
                st.total_opens, 
                st.total_clicks, 
                st.email_sent, 
                cast(st.unique_campaign_open as varchar) as unique_campaign_open,
                cast(st.unique_campaign_click as varchar) as unique_campaign_click, 
                si.id, si.email, si.lofirstname, si.lolastname,
                si.agent, si.purchase_specialist, si.mlo_submission, si.sms_invite_permission,
                si.role, replace(si.loemail,'@pennymac.com','@pnmac.com') as loemail, si.lastname, si.firstname, si.sendgrid,
                si.shorthash, si.utmsource, si.disclosure, si.phonenumber,
                si.utmcampaign, si.lophonenumber, si.customerconnect, si.confirmationrequired,
                t1.closedate,
                t1.lsuclosedate,
                t1.state, t1.city, t1.zip,
                ROW_NUMBER() over (PARTITION BY si.email order by si.invite_date ASC) as row_id
        FROM {{ ref('stg_pmc_invites') }} si
        LEFT OUTER JOIN {{ ref('leads_data_v3') }} t1 on lower(si.email) = lower(t1.client_email)
        LEFT OUTER JOIN {{ ref('pmc_sendgrid_totals') }} st on lower(si.email) = lower(st.to_email)
        LEFT OUTER JOIN {{ ref('partner_user_profiles') }} pup ON LOWER(pup.email) = LOWER(si.loemail)

        UNION

        SELECT
                1 as activity_count,
                'Invite' as activity_type,
                date(t1.system_invite_date) as activity_date,
                t1.id as leadId,
                pil.inviting_nmlsid,
                pil.inviting_lo,
                pil.inviting_aggregate_id AS lo_aggregate_id,
                t1.transaction_type,
                'NA' as lo_contact_attempted,
                case when t1.bank_name is null then 'PennyMac' else t1.bank_name end as partner, 
                'NA' as declined,
                case when t1.system_enroll_date is null then 'FALSE' else 'TRUE' end as enrolled,
                'NA' as declineReason,
                t1.client_name as clientName,
                t1.system_enroll_date as enrolled_date,
                t1.nmlsid as enrollment_lo, 
                t1.client_name, 
                t1.client_email, 
                t1.client_phone, 
                t1.agent_name, 
                t1.agent_email, 
                t1.agent_phone,
                t1.purchase_location, 
                t1.purchase_time_frame, 
                t1.prequal, 
                t1.price_range_lower_bound, 
                t1.price_range_upper_bound, 
                t1.bank_name,
                '0' as total_opens, 
                '0' as total_clicks, 
                '0' as email_sent, 
                '0' as unique_campaign_open,
                '0' as unique_campaign_click,
                '' as id,
                t1.client_email as email,
                pil.inviting_lo_firstname as lofirstname,
                pil.inviting_lo_lastname as lolastname,
                '' as agent,
                '' as purchase_specialist,
                t1.MLOSubmission,
                '' as sms_invite_permission,
                '' as role,
                pil.inviting_lo_email as loemail,
                t1.Client_Last_Name as lastname,
                t1.Client_First_Name as firstname,
                '' as sendgrid,
                '' as shorthash, 
                t1.source as utmsource, 
                '' as disclosure, 
                t1.client_phone as phonenumber,
                '' as utmcampaign, 
                pil.inviting_lo_phone as lophonenumber, 
                '' as customerconnect, 
                '' as confirmationrequired,
                t1.closedate,
                t1.lsuclosedate,
                t1.state, 
                t1.city, 
                t1.zip,
                1 as row_id
        FROM {{ ref('leads_data_v3') }} t1
        LEFT JOIN {{ ref('stg_pennymac_inviting_lo_info') }} pil  on t1.id = pil.lead_id
        WHERE EXTRACT(YEAR FROM t1.system_invite_date) <> '1900' and t1.bank_name = 'PennyMac'
)
, agent_cte AS (
        select
        ca.lead_id,
        ca.profile_aggregate_id
        from {{ ref('current_assignments') }} ca
        where ca.role = 'AGENT'
)
, final_cte AS (
        SELECT 
                cte.*,
                coalesce(cte.lo_aggregate_id,pup.aggregate_id) as lo_aggregate_id_final,
                ac.profile_aggregate_id as agentAggregateID,
                t2.lm_name as ph_lm_name,
                t2.lm_email as ph_lm_email,
                t2.slm_name as ph_slm_name,
                t2.slm_manager as ph_slm_manager,
                t2.dd_name as ph_dd_name,
                t2.dd_email as ph_dd_email,
                t2.division as ph_division,
                t2.lo_nmlsid as ph_lo_nmlsid
        from cte cte
        left outer join {{ source('public', 'raw_partner_hierarchy') }} t2 on t2.lo_email = cte.loemail
        left outer join agent_cte ac on ac.lead_id = cte.leadId
        left outer join {{ ref('partner_user_profiles') }} pup on pup.email = cte.loemail
        where cte.row_id = 1 and lower(cte.loemail) not like '%freedom%'
)
select * from final_cte
