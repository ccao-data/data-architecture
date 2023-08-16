{{ config(materialized='ephemeral') }}

select * from location.other
