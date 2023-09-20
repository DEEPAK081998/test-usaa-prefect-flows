{{
    config(
        materialized = 'table',
        on_schema_change = 'fail',
        tags=['monthly']
    )
}}

{%- set json_rules -%}

'{		  
    "agent_rockscore": [
        {
            "name": "Rock Stars",
            "result": 1,
            "condition": "agent_acceptcount >= 7 AND l15mo_agent_closepct >= 0.25 AND l12mo_agent_responsepct >= 0.5 AND l12mo_agent_acceptpct >= 0.5"
        },
        {
            "name": "Talented Cherry Pickers",
            "result": 2,
            "condition": "agent_acceptcount >= 7 AND l15mo_agent_closepct >= 0.25 AND l12mo_agent_acceptpct < 0.5"
        },
        {
            "name": "Takes All Comers",
            "result": 3,
            "condition": "agent_acceptcount >= 7 AND l15mo_agent_closepct >= 0.195 AND l12mo_agent_responsepct >= 0.6 AND l12mo_agent_acceptpct >= 0.6"
        },
        {
            "name": "Chopping Block",
            "result": 4,
            "condition": "agent_acceptcount >= 6 AND l15mo_agent_closepct <= 0.199"
        },
        {
            "name": "Rising Star",
            "result": 5,
            "condition": "agent_acceptcount <= 6 AND l15mo_agent_closecount >= 2"
        },
        {
            "name": "Unscored",
            "result": 6,
            "condition": "agent_acceptcount <= 6"
        },                                                
        {
            "name": "Other",
            "result": 7,
            "condition": "else"
        }                
    ],
    "agentbank_rockscore": [
        {
            "name": "Rock Stars",
            "result": 1,
            "condition": "agentbank_acceptcount >= 7 AND l15mo_agentbank_closepct >= 0.25 AND l12mo_agentbank_responsepct >= 0.5 AND l12mo_agentbank_acceptpct >= 0.5"
        },
        {
            "name": "Talented Cherry Pickers",
            "result": 2,
            "condition": "agentbank_acceptcount >= 7 AND l15mo_agentbank_closepct >= 0.25 AND l12mo_agentbank_acceptpct < 0.5"
        },
        {
            "name": "Takes All Comers",
            "result": 3,
            "condition": "agentbank_acceptcount >= 7 AND l15mo_agentbank_closepct >= 0.195 AND l12mo_agentbank_responsepct >= 0.6 AND l12mo_agentbank_acceptpct >= 0.6"
        },
        {
            "name": "Chopping Block",
            "result": 4,
            "condition": "agentbank_acceptcount >= 6 AND l15mo_agentbank_closepct <= 0.199"
        },
        {
            "name": "Rising Star",
            "result": 5,
            "condition": "agentbank_acceptcount <= 6 AND l15mo_agentbank_closecount >= 2"
        },
        {
            "name": "Unscored",
            "result": 6,
            "condition": "agentbank_acceptcount <= 6"
        },                                                
        {
            "name": "Other",
            "result": 7,
            "condition": "else"
        }                
    ]
}'

{%- endset -%}

-- calling the macro dynamic_condition_table to create the rockscore condition table
{{ dynamic_condition_table(this, json_rules, versioned=true) }}