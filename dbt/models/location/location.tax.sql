{{ config(materialized='ephemeral') }}

select * from location.tax
