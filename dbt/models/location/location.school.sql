{{ config(materialized='ephemeral') }}

select * from location.school
