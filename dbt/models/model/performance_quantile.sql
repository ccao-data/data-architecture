{{ config(materialized='ephemeral') }}

select * from model.performance_quantile
