{{ config(materialized='ephemeral') }}

select * from census.table_dict
