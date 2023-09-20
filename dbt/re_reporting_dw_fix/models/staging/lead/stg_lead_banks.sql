{{
  config(
    materialized ='view',
    indexes = [
    {'columns': ['bank_id'], 'type': 'hash'},
  ],
    )
}}
WITH distinct_bank_id AS(
  SELECT 
  UPPER(bank_id::VARCHAR) as bank_id
  FROM {{ ref('leads') }}
  GROUP BY
    bank_id
),
mapping_cte AS(
SELECT
  bank_id,
  case
    when bank_id = '25A363D4-0BE0-4001-B9FB-A2F8BA91170C' then 'TD Bank'
    when bank_id = '1085D3EF-3F2B-499A-92C5-71A12DF5A7BA' then 'TMS'
    when bank_id = '1046A4D7-A3AF-4533-8CF0-A02210B94BA1' then 'Alliant'
    when bank_id = '92FAD5D8-A810-4EEC-AE1F-791D07D8F8DA' then 'AmeriSave'
    when bank_id = '3405DC7C-E972-4BC4-A3DA-CB07E822B7C6' then 'Freedom'
    when bank_id = '482C83B7-12F3-4098-A846-B3091A33F966' then 'FreedomVets'
    when bank_id = '05EFF75C-B274-49B5-94A0-9F47621D5D16' then 'Lakeview'
    when bank_id = '9C4D692D-CCCA-42C2-8CE7-3DF13511498F' then 'HomePoint'
    when bank_id = 'A1443203-486E-42A1-8C2F-01AF57D0295C' then 'Regions'
    when bank_id = '06D7EEA6-A312-4233-B53B-DE52EA1C240E' then 'Citizens'
    when bank_id = '7AC24FE8-8709-41BA-8F0B-77837969200A' then 'Chase'
    when bank_id = '6E15BA77-A19A-4CCC-ACC6-3AA3601FCBF9' then 'RBC'
    when bank_id = 'EDDCA1B5-D6FD-4F68-996C-BCDCBEA19EF9' then 'HSBC'
    when bank_id = '2D662A5F-8415-459C-B2D3-44F287E1FACD' then 'BBVA'
    when bank_id = '3EEAEF5F-6F12-4B42-BB7B-3B0EB74E4B1F' then 'Fidelity'
    when bank_id = '4377AEDD-2BA1-41EF-9D10-0E522738FD7A' then 'Zillow'
    when bank_id = '77DACCC1-1178-4FD2-B95E-4F291476CBD9' then 'SoFi'
    when bank_id = 'E2A46D0A-6544-4116-8631-F08D749045AC' then 'PennyMac'
    when bank_id = 'C124C142-A3BA-45BC-9A6F-493BCF386C0F' then 'Kitsap'
    when bank_id = '2DCA0E1B-DAD1-4164-B440-7BC716BDF56D' then 'HSF'
    else 'Error'
  end as bank_name
FROM distinct_bank_id)
SELECT
{% if target.type == 'snowflake' %}
bank_id,
bank_name
{% else %}
bank_id::UUID as bank_id,
bank_name
{% endif %}
FROM 
mapping_cte