{{ config(materialized='ephemeral') }}

select * from location.political
