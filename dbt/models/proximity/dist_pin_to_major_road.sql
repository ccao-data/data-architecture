{{ config(materialized='ephemeral') }}

select * from proximity.dist_pin_to_major_road
