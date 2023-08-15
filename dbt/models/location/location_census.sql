{{ config(materialized='ephemeral', alias='census') }}

select * from location.census
