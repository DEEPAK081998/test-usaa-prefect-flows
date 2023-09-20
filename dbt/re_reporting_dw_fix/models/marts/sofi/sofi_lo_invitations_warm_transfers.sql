select 1 as activity_count, 'Warm Transfer' as activity_type, date(wt.date) as activity_date, wt.leadid::bigint as leadId, wt.nmlsid as inviting_nmlsid, wt.loan_officer_name as inviting_lo, wt.lo_contact_attempted,
       wt.partner as partner, wt.declined, wt.enrolled, wt.decline_reason, wt.client_name as invitee_name,
       date({{ calculate_time_interval('ld.created', '-', '7', 'hour') }}) as enrolled_date, ld.nmlsid as enrollment_lo, ld.lo_name as enrollment_lo_name, ld.lo_phone as enrollment_lo_phone, ld.lo_email as enrollment_lo_email, ld.client_name, ld.client_email, ld.client_phone, ld.agent_name, ld.agent_email, ld.agent_phone,
       ld.purchase_location, ld.purchase_time_frame, ld.prequal, ld.price_range_lower_bound, ld.price_range_upper_bound, ld.bank_name,
       '0' as total_opens, '0' as total_clicks, '0' as email_sent, CAST('0' AS VARCHAR(10)) as unique_campaign_open,
        CAST('0' AS VARCHAR(10)) as unique_campaign_click,
        '' as invite_id, '' as invite_email, '' as lofirstname, '' as lolastname,
        '' as agent, '' as purchase_specialist, '' as mlo_submission, '' as sms_invite_permission,
        '' as role, '' as loemail, '' as lastname, '' as firstname, '' as sendgrid,
        '' as shorthash, '' as utmsource, '' as disclosure, '' as phonenumber,
        '' as utmcampaign, '' as lophonenumber, '' as customerconnect, '' as confirmationrequired,
        ld.closedate, ld.lsuclosedate,
        ld.hb_status as hb_status,
        ld.hb_status_time as hb_status_time,
        ld.last_agent_update,
        ld.transaction_type,
        phh.lm_name as ph_lm_name,
        phh.lm_email as ph_lm_email,
        phh.slm_name as ph_slm_name,
        phh.slm_manager as ph_slm_manager,
        phh.dd_name as ph_dd_name,
        phh.dd_email as ph_dd_email,
        phh.division as ph_division
from {{ ref('warm_transferlog') }} wt
left outer join {{ ref('leads_data_v3') }} ld on wt.leadid::int = ld.id::int
left outer join {{ source('public', 'raw_partner_hierarchy') }} phh on phh.lo_nmlsid = wt.nmlsid OR phh.lo_name = wt.loan_officer_name
where wt.date is not null
and trim(lower(partner)) = 'sofi'
union
select 1 as activity_count, 'Email' as activity_type, date(si.invite_date) as activity_date, t1.id as leadId, si.nmlsid as inviting_nmlsid,concat(si.lofirstname,' ', si.lolastname) as inviting_lo, 'NA' as lo_contact_attempted,
        case when t1.bank_name is null then 'SoFi' else t1.bank_name end as partner, 'NA' as declined, 
        case when t1.id is null then 'FALSE' else 'TRUE' end as enrolled,
        'NA' as declineReason, concat( si.firstname, ' ',si.lastname) as invitee_name,
        date({{ calculate_time_interval('t1.created', '-', '7', 'hour') }}) as enrolled_date,
        t1.nmlsid as enrollment_lo, t1.lo_name as enrollment_lo_name, t1.lo_phone as enrollment_lo_phone, t1.lo_email as enrollment_lo_email,
        t1.client_name, t1.client_email, t1.client_phone, t1.agent_name, t1.agent_email, t1.agent_phone,
       t1.purchase_location, t1.purchase_time_frame, t1.prequal, t1.price_range_lower_bound, t1.price_range_upper_bound, t1.bank_name,
        st.total_opens, st.total_clicks, st.email_sent,
        cast(st.unique_campaign_open as varchar) as unique_campaign_open,
        cast(st.unique_campaign_click as varchar) as unique_campaign_click,
        si.id, si.email, si.lofirstname, si.lolastname,
        si.agent, si.purchase_specialist, si.mlo_submission, si.sms_invite_permission,
        si.role, si.loemail, si.lastname, si.firstname, si.sendgrid,
        si.shorthash, si.utmsource, si.disclosure, si.phonenumber,
        si.utmcampaign, si.lophonenumber, si.customerconnect, si.confirmationrequired,
        t1.closedate,
        t1.lsuclosedate,
        t1.hb_status as hb_status,
        t1.hb_status_time as hb_status_time,
        t1.last_agent_update,
        t1.transaction_type,
        ph.lm_name as ph_lm_name,
        ph.lm_email as ph_lm_email,
        ph.slm_name as ph_slm_name,
        ph.slm_manager as ph_slm_manager,
        ph.dd_name as ph_dd_name,
        ph.dd_email as ph_dd_email,
        ph.division as ph_division
from {{ ref('stg_sofi_invites') }} si
left outer join {{ ref('leads_data_v3') }} t1 on lower(si.email) = lower(t1.client_email)
left outer join {{ ref('sofi_sendgrid_totals') }} st on lower(si.email) = lower(st.to_email)
left outer join {{ source('public', 'raw_partner_hierarchy') }} ph on ph.lo_name = concat(si.lofirstname,' ', si.lolastname) OR ph.lo_email = si.loemail