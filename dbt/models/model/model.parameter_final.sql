{{ config(materialized='ephemeral') }}

select * from model.parameter_final
