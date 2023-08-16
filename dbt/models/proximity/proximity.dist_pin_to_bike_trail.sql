{{ config(materialized='ephemeral') }}

select * from proximity.dist_pin_to_bike_trail
