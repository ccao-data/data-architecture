{{ config(materialized='ephemeral') }}

select * from census.decennial
