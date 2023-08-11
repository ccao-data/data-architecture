{{ config(materialized='ephemeral') }}

select * from census.variable_dict
