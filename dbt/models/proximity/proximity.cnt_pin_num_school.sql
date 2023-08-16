{{ config(materialized='ephemeral') }}

select * from proximity.cnt_pin_num_school
