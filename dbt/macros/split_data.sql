{% macro create_dist_to_nearest_cte(township_code) %}
    filtered_joined_traffic_township_{{ township_code }} as (
        select *
        from {{ dist_to_nearest_geometry("filtered_joined_traffic_township") }}
        where township_code = {{ township_code }}
    )
{% endmacro %}
