{{ config(materialized='ephemeral') }}

select * from model.feature_importance
