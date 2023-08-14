{{ config(materialized='ephemeral') }}

select * from model.final_model
