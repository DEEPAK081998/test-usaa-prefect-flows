{{
  config(
    materialized = 'view',
    indexes=[
	  {'columns': ['bank_id'], 'type': 'hash'},
	],
    )
}}
SELECT
  DISTINCT
  partner_id,
	case
	 	when partner_id = '25A363D4-0BE0-4001-B9FB-A2F8BA91170C' then 'TD Bank'
    when partner_id = '1085d3ef-3f2b-499a-92c5-71a12df5a7ba' then 'TMS'
    when partner_id = '1046a4d7-a3af-4533-8cf0-a02210b94ba1' then 'Alliant'
    when partner_id = '92fad5d8-a810-4eec-ae1f-791d07d8f8da' then 'AmeriSave'
    when partner_id = '3405dc7c-e972-4bc4-a3da-cb07e822b7c6' then 'Freedom'
    when partner_id = '482c83b7-12f3-4098-a846-b3091a33f966' then 'FreedomVets'
    when partner_id = '05eff75c-b274-49b5-94a0-9f47621d5d16' then 'Lakeview'
    when partner_id = '9c4d692d-ccca-42c2-8ce7-3df13511498f' then 'HomePoint'
    when partner_id = 'a1443203-486e-42a1-8c2f-01af57d0295c' then 'Regions'
    when partner_id = '06D7EEA6-A312-4233-B53B-DE52EA1C240E' then 'Citizens'
    when partner_id = '7AC24FE8-8709-41BA-8F0B-77837969200A' then 'Chase'
    when partner_id = '6E15BA77-A19A-4CCC-ACC6-3AA3601FCBF9' then 'RBC'
    when partner_id = 'EDDCA1B5-D6FD-4F68-996C-BCDCBEA19EF9' then 'HSBC'
    when partner_id = '2D662A5F-8415-459C-B2D3-44F287E1FACD' then 'BBVA'
    when partner_id = '3EEAEF5F-6F12-4B42-BB7B-3B0EB74E4B1F' then 'Fidelity'
    when partner_id = '4377AEDD-2BA1-41EF-9D10-0E522738FD7A' then 'Zillow'
    when partner_id = '77DACCC1-1178-4FD2-B95E-4F291476CBD9' then 'SoFi'
    when partner_id = 'E2A46D0A-6544-4116-8631-F08D749045AC' then 'PennyMac'
    when partner_id = 'C124C142-A3BA-45BC-9A6F-493BCF386C0F' then 'Kitsap'
    when partner_id = '2dca0e1b-dad1-4164-b440-7bc716bdf56d' then 'HSF'
    else 'Error'
	end as bank_name
FROM {{ ref('partner_user_profiles') }}