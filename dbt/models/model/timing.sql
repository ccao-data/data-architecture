{{ config(materialized='ephemeral') }}

select * from model.timing
