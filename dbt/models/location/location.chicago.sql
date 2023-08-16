{{ config(materialized='ephemeral') }}

select * from location.chicago
