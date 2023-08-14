{{ config(materialized='ephemeral') }}

select * from model.metadata
