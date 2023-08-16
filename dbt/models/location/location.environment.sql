{{ config(materialized='ephemeral') }}

select * from location.environment
