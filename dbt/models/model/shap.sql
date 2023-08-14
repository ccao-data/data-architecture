{{ config(materialized='ephemeral') }}

select * from model.shap
