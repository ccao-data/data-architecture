{{ config(materialized='ephemeral') }}

select * from model.test_card
