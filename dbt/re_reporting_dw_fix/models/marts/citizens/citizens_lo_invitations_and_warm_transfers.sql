{{
  config(
    materialized = 'table',
    )
}}
WITH 
    lo_cte AS (
        SELECT
            ca.lead_id,
            ca.profile_aggregate_id AS mlo_aggregate_id,
            CONCAT(pup.first_name,' ',pup.last_name) AS inviting_lo,
            pup.first_name AS inviting_lo_firstname,
            pup.last_name AS inviting_lo_lastname,
            {% if target.type == 'snowflake' %}
            pup.phones[0].phoneNumber::varchar AS inviting_lo_phone,
            pup.email AS inviting_lo_email,
            pup.data:nmlsid::VARCHAR AS mlo_nmlsid,
            {% else %}
            pup.phones->0->> 'phoneNumber' AS inviting_lo_phone,
            pup.email AS inviting_lo_email,
            pup.data->> 'nmlsid' AS mlo_nmlsid,
            {% endif %}
            ROW_NUMBER() over (PARTITION BY ca.lead_id ORDER BY ca.created ASC) AS row_id
        FROM
            {{ ref('current_assignments') }} ca
        LEFT JOIN {{ ref('partner_user_profiles') }} pup
            ON pup.aggregate_id = ca.profile_aggregate_id
        WHERE
            ca.role = 'MLO'
    )
    , inviting_lo_cte AS (
        SELECT
            lsu.lead_id,
            loc.inviting_lo_email,
            loc.inviting_lo,
            loc.inviting_lo_firstname,
            loc.inviting_lo_lastname,
            loc.inviting_lo_phone,
            loc.mlo_aggregate_id AS inviting_aggregate_id,
            loc.mlo_nmlsid AS inviting_nmlsid
        FROM
            {{ ref('lead_status_updates') }} lsu
            LEFT JOIN lo_cte loc
            ON loc.lead_id = lsu.lead_id
        WHERE
            lsu.status LIKE '%Invited%' AND row_id = 1
        ORDER BY
            lead_id ASC
    )
    , pop_data AS (
        {% if target.type == 'snowflake' %}
            SELECT aggregate_id, fp.value:phoneType::VARCHAR as PhoneType, fp.value:phoneNumber::VARCHAR as phoneNumber
            FROM {{ ref('partner_user_profiles') }} pup,
            lateral flatten(input => pup.phones) fp
        {% else %}
            SELECT aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType, json_array_elements(phones)->>'phoneNumber' as phoneNumber
            FROM {{ ref('partner_user_profiles') }}
        {% endif %}
    )
    , aop_cte AS (
        SELECT aggregate_id, min(PhoneNumber) as OfficePhone
        FROM pop_data pop
        WHERE lower(pop.Phonetype) = 'office'
        GROUP BY aggregate_id
    )
    , amp_cte AS (
        SELECT aggregate_id, min(PhoneNumber) as MobilePhone
        FROM pop_data pop
        WHERE lower(pop.Phonetype) = 'mobilephone'
        GROUP BY aggregate_id
    )
    , mlo_hierarchy_cte AS (
        SELECT
            'Citizens' AS bank_name,
            
            CONCAT(pup.first_name,' ',pup.last_name) AS mlo_name,
            pup.email AS mlo_email,
            COALESCE(mop.OfficePhone,mmp.MobilePhone) AS mlo_phone,
            {% if target.type == 'snowflake' %}
            pup.data:nmlsid::VARCHAR AS mlo_nmlsid,
            pup.data:apsm:managerName::VARCHAR AS apsm_manager_name,
            pup.data:apsm:email::VARCHAR AS apsm_email,
            pup.data:psm:managerName::VARCHAR AS psm_manager_name,
            pup.data:psm:email::VARCHAR AS psm_email,
            pup.data:asm:name::VARCHAR AS asm_region,
            pup.data:asm:managerName::VARCHAR AS asm_manager_name,
            pup.data:asm:email::VARCHAR AS asm_email,
            pup.data:rsm:name::VARCHAR AS rsm_region,
            pup.data:rsm:managerName::VARCHAR AS rsm_manager_name,
            pup.data:rsm:email::VARCHAR AS rsm_email,
            pup.data:dsm:managerName::VARCHAR AS dsm_manager_name,
            pup.data:dsm:name::VARCHAR AS dsm_division,
            pup.data:dsm:email::VARCHAR AS dsm_email,
            pup.aggregate_id,
            'true' AS enabled,
            pure.role,
            pup.updated,
            ROW_NUMBER() over (PARTITION BY pup.data:nmlsid::VARCHAR ORDER BY pup.updated DESC) AS row_id
            {% else %}
            pup.data ->> 'nmlsid' AS mlo_nmlsid,
            pup.data -> 'apsm' ->> 'managerName' AS apsm_manager_name,
            pup.data -> 'apsm' ->> 'email' AS apsm_email,
            pup.data -> 'psm' ->> 'managerName' AS psm_manager_name,
            pup.data -> 'psm' ->> 'email' AS psm_email,
            pup.data -> 'asm' ->> 'name' AS asm_region,
            pup.data -> 'asm' ->> 'managerName' AS asm_manager_name,
            pup.data -> 'asm' ->> 'email' AS asm_email,
            pup.data -> 'rsm' ->> 'name' AS rsm_region,
            pup.data -> 'rsm' ->> 'managerName' AS rsm_manager_name,
            pup.data -> 'rsm' ->> 'email' AS rsm_email,
            pup.data -> 'dsm' ->> 'managerName' AS dsm_manager_name,
            pup.data -> 'dsm' -> 'name' AS dsm_division,
            pup.data -> 'dsm' -> 'email' AS dsm_email,
            pup.aggregate_id,
            'true' AS enabled,
            pure.role,
            pup.updated,
            ROW_NUMBER() over (PARTITION BY pup.data ->> 'nmlsid' ORDER BY pup.updated DESC) AS row_id
            {% endif %}
        FROM
            {{ ref('partner_user_profiles') }} pup
            LEFT JOIN {{ ref('partner_user_roles') }} pure
            ON pure.user_profile_id = pup.id
            LEFT OUTER JOIN aop_cte mop
            ON mop.aggregate_id = pup.aggregate_id
            LEFT OUTER JOIN amp_cte mmp
            ON mmp.aggregate_id = pup.aggregate_id
        WHERE
            email LIKE '%citizens%'
            AND pure.role = 'MLO'
    )
    , mlo_final AS (
        SELECT
            *
        FROM
            mlo_hierarchy_cte
        WHERE
            row_id = 1
    )
    , base_cte AS (
        SELECT
            ilc.*,
            ld.*,
            hc.apsm_manager_name AS inviting_aspm_manager_name,
            hc.apsm_email AS inviting_aspm_email,
            hc.psm_manager_name AS inviting_psm_manager_name,
            hc.psm_email AS inviting_psm_email,
            hc.asm_region AS inviting_asm_region,
            hc.asm_manager_name AS inviting_asm_manager_name,
            hc.asm_email AS inviting_asm_email,
            hc.rsm_region AS inviting_rsm_region,
            hc.rsm_manager_name AS inviting_rsm_manager_name,
            hc.rsm_email AS inviting_rsm_email,
            hc.dsm_manager_name AS inviting_dsm_manager_name,
            hc.dsm_email AS inviting_dsm_email,
            hc.dsm_division AS inviting_dsm_division,
            mhc.apsm_manager_name AS enrollment_aspm_manager_name,
            mhc.apsm_email AS enrollment_aspm_email,
            mhc.psm_manager_name AS enrollment_psm_manager_name,
            mhc.psm_email AS enrollment_psm_email,
            mhc.asm_region AS enrollment_asm_region,
            mhc.asm_manager_name AS enrollment_asm_manager_name,
            mhc.asm_email AS enrollment_asm_email,
            mhc.rsm_region AS enrollment_rsm_region,
            mhc.rsm_manager_name AS enrollment_rsm_manager_name,
            mhc.rsm_email AS enrollment_rsm_email,
            mhc.dsm_manager_name AS enrollment_dsm_manager_name,
            mhc.dsm_email AS enrollment_dsm_email,
            mhc.dsm_division AS enrollment_dsm_division
        FROM
            inviting_lo_cte ilc 
            FULL JOIN {{ ref('leads_data_v3') }} ld
            ON ld.id = ilc.lead_id
            LEFT JOIN mlo_final mhc
            ON mhc.mlo_nmlsid = ld.nmlsid
            LEFT JOIN mlo_final hc
            ON hc.mlo_nmlsid = ilc.inviting_nmlsid
        WHERE
            ld.bank_name = 'Citizens'
        ORDER BY
            ilc.lead_id ASC
    )
    , int_cte AS (
        SELECT
            1 AS activity_count,
            'Warm Transfer' AS activity_type,
            DATE(wt.date),
            wt.leadid::bigint AS leadId,
            wt.nmlsid AS inviting_nmlsid,
            wt.loan_officer_name AS inviting_lo,
            wt.lo_contact_attempted,
            wt.partner AS partner,
            wt.declined,
            wt.enrolled,
            wt.decline_reason,
            wt.client_name AS invitee_name,
            DATE(ld.created - INTERVAL '7 Hours') AS enrolled_date,
            ld.nmlsid AS enrollment_lo,
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
            '0' AS total_opens,
            '0' AS total_clicks,
            '0' AS email_sent,
            '0' AS unique_campaign_open,
            '0' AS unique_campaign_click,
            '' AS invite_id,
            '' AS invite_email,
            /* '' as lofirstname, '' as lolastname,*/
            '' AS AGENT,
            '' AS purchase_specialist,
            '' AS mlo_submission,
            '' AS sms_invite_permission,
            '' AS role,
            '' AS loemail,
            '' AS lastname,
            '' AS firstname,
            '' AS sendgrid,
            '' AS shorthash,
            '' AS utmsource,
            '' AS disclosure,
            '' AS phonenumber,
            '' AS utmcampaign,
            '' AS lophonenumber,
            '' AS customerconnect,
            '' AS confirmationrequired,
            ld.closedate,
            ld.lsuclosedate,
            ld.hb_status
            /*,
                    ld.hb_status as hb_status,
                    phh.lm_name as ph_lm_name,
                    phh.lm_email as ph_lm_email,
                    phh.slm_name as ph_slm_name,
                    phh.slm_manager as ph_slm_manager,
                    phh.dd_name as ph_dd_name,
                    phh.dd_email as ph_dd_email,
                    phh.division as ph_division*/
        FROM
            {{ ref('warm_transferlog') }} wt
            LEFT OUTER JOIN {{ ref('leads_data_v3') }} ld
            ON wt.leadid::INT = ld.id::INT --left outer join raw_partner_hierarchy phh on phh.lo_nmlsid = wt.nmlsid OR phh.lo_name = wt.loan_officer_name
        WHERE
            wt.date IS NOT NULL AND TRIM(LOWER(partner)) = 'citizens'

        UNION

        SELECT
            1 AS activity_count,
            'Invite' AS activity_type,
            DATE(pd.system_invite_date) AS activity_date,
            pd.id AS leadId,
            pd.inviting_nmlsid AS inviting_nmlsid,
            pd.inviting_lo AS inviting_lo,
            'NA' AS lo_contact_attempted,
            CASE
                WHEN pd.bank_name IS NULL THEN 'Citizens'
                ELSE pd.bank_name
            END AS partner,
            'NA' AS declined,
            CASE
                WHEN pd.system_enroll_date IS NULL THEN 'FALSE'
                ELSE 'TRUE'
            END AS enrolled,
            'NA' AS declineReason,
            pd.client_name AS invitee_name,
            pd.system_enroll_date AS enrolled_date,
            pd.nmlsid AS enrollment_lo,
            pd.client_name,
            pd.client_email,
            pd.client_phone,
            pd.agent_name,
            pd.agent_email,
            pd.agent_phone,
            pd.purchase_location,
            pd.purchase_time_frame,
            pd.prequal,
            pd.price_range_lower_bound,
            pd.price_range_upper_bound,
            pd.bank_name,
            '0' AS total_opens,
            '0' AS total_clicks,
            '0' AS email_sent,
            '0' AS unique_campaign_open,
            '0' AS unique_campaign_click,
            '' AS invite_id,
            pd.inviting_lo_email AS invite_email,
            /*pd.inviting_lo_firstname as lofirstname, pd.inviting_lo_lastname as lolastname,*/
            '' AS AGENT,
            '' AS purchase_specialist,
            pd.mlosubmission AS mlo_submission,
            '' AS sms_invite_permission,
            '' AS role,
            pd.inviting_lo_email AS invite_email,
            pd.client_last_name AS lastname,
            pd.client_first_name AS firstname,
            '' AS sendgrid,
            '' AS shorthash,
            pd.source AS utmsource,
            '' AS disclosure,
            pd.client_phone AS phonenumber,
            '' AS utmcampaign,
            pd.inviting_lo_phone AS lophonenumber,
            '' AS customerconnect,
            '' AS confirmationrequired,
            pd.closedate,
            pd.lsuclosedate,
            pd.hb_status AS hb_status
            /*,
                    ph.lm_name as ph_lm_name,
                    ph.lm_email as ph_lm_email,
                    ph.slm_name as ph_slm_name,
                    ph.slm_manager as ph_slm_manager,
                    ph.dd_name as ph_dd_name,
                    ph.dd_email as ph_dd_email,
                    ph.division as ph_division*/
        FROM
            base_cte pd --left outer join raw_partner_hierarchy ph on ph.lo_name = concat(si.lofirstname,' ', si.lolastname) OR ph.lo_nmlsid = si.nmlsid or ph.lo_email = si.loemail
        WHERE
            EXTRACT(YEAR FROM pd.system_invite_date ) <> '1900'
            AND pd.bank_name = 'Citizens'
    )
    , final_cte AS (
        SELECT 
            a.*,
             {{ dbt_utils.star(from=ref('all_partner_hierarchy'), except=["bank_name"],relation_alias='b') }}
        FROM
            int_cte a
            LEFT JOIN {{ ref('all_partner_hierarchy') }} b
            on a.inviting_nmlsid = b.mlo_nmlsid
    )
    SELECT * FROM final_cte