{% macro create_functions() %}

{% set lead_rejected %}
  CREATE OR REPLACE FUNCTION lead_rejected(lead_id bigint)
    RETURNS BIGINT AS $$
    DECLARE 
        lead_rejected_count BIGINT;
    BEGIN
        SELECT COUNT(*)
        INTO lead_rejected_count
        FROM lead_status_updates lsu
        INNER JOIN partner_user_profiles pup
        ON lsu.profile_aggregate_id = pup.aggregate_id
        WHERE lsu.lead_id = $1 AND lsu.status = 'Rejected Rejected' 
        AND (lsu.role = 'AGENT' OR lsu.role = 'REFERRAL_COORDINATOR');
        
        RETURN lead_rejected_count;
    END;
    $$ LANGUAGE plpgsql;
{% endset %}

{% set concierge_escalated %}
  CREATE OR REPLACE FUNCTION concierge_escalated(lead_id bigint)
    RETURNS BIGINT AS $$
    DECLARE 
        concierge_escalated_count BIGINT;
    BEGIN
        SELECT COUNT(*)
        INTO concierge_escalated_count
        FROM lead_status_updates lsu
        WHERE lsu.lead_id = $1 AND lsu.status = 'New New Referral' AND lsu.category LIKE 'Property%';
        
        IF concierge_escalated_count > 0 then
            concierge_escalated_count = 1;
        END IF;
        
        RETURN concierge_escalated_count;
    END;
    $$ LANGUAGE plpgsql;
{% endset %}

{% set first_call %}
    CREATE OR REPLACE FUNCTION first_call(lead_id bigint)
    RETURNS timestamp without time zone AS $$
    DECLARE 
        first_call_date timestamp without time zone;
    BEGIN
        SELECT lsu.created
        INTO first_call_date
        FROM lead_status_updates lsu
        INNER JOIN partner_user_profiles pup
        ON lsu.profile_aggregate_id = pup.aggregate_id
        WHERE lsu.lead_id = $1 AND lsu.status = 'Outreach Click to Call' 
        AND lsu.category = 'Activities' AND (lsu.role = 'AGENT' OR lsu.role = 'REFERRAL_COORDINATOR')
        ORDER BY lsu.created;
        
        RETURN first_call_date;
    END;
    $$ LANGUAGE plpgsql;
{% endset %}

{% set total_assignments %}
    CREATE OR REPLACE FUNCTION {{ target.schema}}.total_assignments(lead_id bigint)
    RETURNS BIGINT AS $$
    DECLARE 
        total_assignments_count BIGINT;
    BEGIN
        SELECT COUNT(DISTINCT(profile_aggregate_id))
        INTO total_assignments_count
        FROM profile_assignments pa
        WHERE pa.lead_id = $1 AND (pa.role = 'AGENT' OR pa.role = 'REFERRAL_COORDINATOR');
        
        RETURN total_assignments_count;
    END;
    $$ LANGUAGE plpgsql;
{% endset %}

{{ log('lead_rejected: ' ~ lead_rejected, info=False) }}
{% do run_query(lead_rejected) %}
{{ log('concierge_escalated: ' ~ concierge_escalated, info=False) }}
{% do run_query(concierge_escalated) %}
{{ log('first_call: ' ~ first_call, info=False) }}
{% do run_query(first_call) %}
{{ log('total_assignments: ' ~ total_assignments, info=False) }}
{% do run_query(total_assignments) %}
    
{% endmacro %}



{% macro total_assignments() %}

{% endmacro %}