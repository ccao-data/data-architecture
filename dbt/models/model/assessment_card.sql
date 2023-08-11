{{ config(materialized='ephemeral') }}

select * from model.assessment_card
