{% macro aggregate_lowest_distance(table_name) %}
    with
        distances as (
            -- Unpivot all "dist_ft" columns for comparison
            select
                *,
                unnest(
                    array[
                        {% for column in dbt_utils.get_filtered_columns_in_relation(
                            table_name, "dist_ft"
                        ) %}
                            ({{ column }}, '{{ column }}')
                        {% endfor %}
                    ]
                ) as (distance_value, distance_column)
            from {{ table_name }}
        ),
        min_distances as (
            -- Identify the lowest distance and corresponding column for each row
            select
                *,
                min(distance_value) over (partition by id) as min_distance,
                array_agg(distance_column) filter (
                    where distance_value = min(distance_value) over (partition by id)
                ) as matching_columns
            from distances
        ),
        aggregated as (
            -- Extract the prefix before the first underscore and rename it with
            -- "aggregated_"
            select
                id,
                min_distance,
                array_agg(
                    distinct regexp_extract(column_name, '^(.*?)_')
                ) as original_prefixes,
                array_agg(
                    distinct 'aggregated_' || regexp_extract(column_name, '^(.*?)_')
                ) as aggregated_prefixes
            from
                (
                    select id, unnest(matching_columns) as column_name
                    from min_distances
                ) prefix_agg
            group by id, min_distance
        )
    select *
    from aggregated
    ;
{% endmacro %}
