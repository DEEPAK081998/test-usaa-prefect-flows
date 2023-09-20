{{ config(materialized='table') }}

SELECT
date(t1.created),
t1.id,
case when t2.email is null then 'NULL' else t2.email end as agent_email,
case when t2.email is null then 'NULL' else t2.email end as agent_email2,
--t2.phone as agent_phone,
--t2.fullName as agent_name,
case when t3.email is null then 'NULL' else t3.email end as lo_email,
case when t3.email is null then 'NULL' else t3.email end as lo_email2,
t1.purchase_location,
t1.purchase_location_detail,
t1.purchase_time_frame,
t1.prequal,
t1.price_range_lower_bound,
t1.price_range_upper_bound,
t1.comments,
case when t1.bank_id = '25A363D4-0BE0-4001-B9FB-A2F8BA91170C' then 'TD Bank'
when t1.bank_id = '1085d3ef-3f2b-499a-92c5-71a12df5a7ba' then 'TMS'
when t1.bank_id = '1046a4d7-a3af-4533-8cf0-a02210b94ba1' then 'Alliant'
when t1.bank_id = '92fad5d8-a810-4eec-ae1f-791d07d8f8da' then 'AmeriSave'
when t1.bank_id = '3405dc7c-e972-4bc4-a3da-cb07e822b7c6' then 'Freedom'
when t1.bank_id = '482c83b7-12f3-4098-a846-b3091a33f966' then 'FreedomVets'
when t1.bank_id = '05eff75c-b274-49b5-94a0-9f47621d5d16' then 'Lakeview'
when t1.bank_id = '9c4d692d-ccca-42c2-8ce7-3df13511498f' then 'HomePoint'
when t1.bank_id = 'A1443203-486E-42A1-8C2F-1AF57D0295C' then 'Regions'
when t1.bank_id = '06D7EEA6-A312-4233-B53B-DE52EA1C240E' then 'Citizens'
else 'Error' end as bank_name,
case when t1.transaction_type = 'PURCHASE' then 'BUY'
when t1.transaction_type = 'BOTH' then 'BUY' else t1.transaction_type end as transaction_type
FROM leads_view t1
left outer join (select lead_id, email, profile->'phone' as phone,
                profile->'fullName' as fullName
                from current_assignments_view
                where role = 'AGENT') t2 on t2.lead_id = t1.id
left outer join (select lead_id, email, profile->'phone',
                profile->'fullName' as fullName
                from current_assignments_view
                where role = 'MLO') t3 on t3.lead_id = t1.id
WHERE t1.id not in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,
15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,
32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,
50,51,52,53,54,55,56,68,71,72,73,78,79,80,82,89,94,97,98,99,
100,101,102,103,104,105,106,107,109,110,111,112,113,114,118,
120,125,133,135,142,176,181,182,183,188,190,195,198,
205,209,218,225,227,231,245,246,247,248,249,254,261,266,
271,276,292,293,298,417,419,420,422,423,435,458,536, 540,577,586,587,
722,723,727,728,730,731,740,756,832,833,834,915,1294,1545,1773,1765,
1766,1764,1854,1843,1894,1891,1890,1889,1888,1884,1944,
1935,1934,1933,1922,1921)
and t1.bank_id not in ('25A363D4-0BE0-4001-B9FB-A2F8BA91170C')
and date(t1.created) > '2017-09-01'
ORDER BY t1.id desc