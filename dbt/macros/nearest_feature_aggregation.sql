{% macro aggregate_smallest_feature(table_name, feature_suffix="dist_ft") %}
    {%- set macro_content -%}
        {% raw %}
        {% macro aggregate_smallest_feature(table_name, feature_suffix="dist_ft") %}
            with
                columns as (
                    select column_name
                    from information_schema.columns
                    where
                        table_name = lower('{{ table_name }}')
                        and column_name like '%' || '{{ feature_suffix }}'
                ),
                feature as (
                    select
                        t.*,
                        unnest(
                            array[
                                {% for column in columns %}
                                    (t.{{ column.column_name }}, '{{ column.column_name }}')
                                {% endfor %}
                            ]
                        ) as (feature_value, feature_column)
                    from {{ table_name }} t
                ),
                min_feature as (
                    select
                        *,
                        min(feature_value) over (partition by id) as min_feature,
                        array_agg(feature_column) filter (
                            where feature_value = min(feature_value) over (partition by id)
                        ) as matching_columns
                    from feature
                ),
                aggregated as (
                    select
                        id,
                        min_feature,
                        array_agg(
                            distinct regexp_extract(column_name, '^(.*?)_')
                        ) as original_prefixes,
                        array_agg(
                            distinct 'aggregated_' || regexp_extract(column_name, '^(.*?)_')
                        ) as aggregated_prefixes
                    from
                        (
                            select id, unnest(matching_columns) as column_name from min_feature
                        ) prefix_agg
                    group by id, min_feature
                )
            select *
            from aggregated
            ;
        {% endmacro %}
        {% endraw %}
    {%- endset -%}

    {{ macro_content }}
{% endmacro %}
