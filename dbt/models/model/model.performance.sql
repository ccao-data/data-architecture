{{ config(materialized='ephemeral') }}

select * from model.performance
