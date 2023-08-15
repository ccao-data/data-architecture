{{ config(materialized='ephemeral') }}

select * from location.access
