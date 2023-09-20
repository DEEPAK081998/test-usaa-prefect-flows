with cte AS (
    select 
        st.*,
        ph.level_2_manager as ph_lm_name,
        ph.level_2_manager_email as ph_lm_email,
        ROW_NUMBER() over (PARTITION BY 
        st.id order by st.created DESC) as row_id
    from {{ ref('sofi_rec_w_statuses') }} st
    left join {{ ref("all_partner_hierarchy") }} ph
    on ph.mlo_nmlsid = st.nmlsid
)
, final AS (
    select *,
        case when row_id = 2 then status else hb_status end as previous_status,
        case when lower(HB_Status) like '%closed%' then 'Closed'
        when lower(HB_Status) like 'active%' then 'Active'
        when lower(HB_Status) like '%pending%' then 'Pending'
        when lower(HB_Status) like 'inactive%' then 'Inactive'
    else 'Active' end as major_status
    from cte
)
,
final_f AS (
    select id,
    status as previousStatus
    from final
    where row_id = 2
)
select f.*,
ff.previousStatus
from final f
left join final_f ff on ff.id = f.id