{{ config(materialized='ephemeral') }}

select * from rpie.pin_codes_dummy
