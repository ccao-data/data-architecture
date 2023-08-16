{{ config(materialized='ephemeral') }}

select * from model.assessment_pin
