-- Macro that takes a model and returns its row count grouped and
-- sorted by a given column. The sort order for the results can be specified
-- with the `ordering` argument, defaulting to "asc".
--
-- If the `print` argument is set to True (default is False), the macro will
-- print the results of the query to stdout, which allows this macro to be used
-- by scripts to return data.
{% macro row_count_by_group(model, group_by, ordering="asc", print=False) %}
    {%- set columns_csv = group_by | join(", ") %}

    {% set query %}
        select count(*) as COUNT, {{ columns_csv }}
        from {{ model }}
        group by {{ columns_csv }}
        order by {{ columns_csv }} {{ ordering }}
    {% endset %}

    {% if print %}
        {% set results = run_query(query) %} {{ results.print_json() }}
    {% endif %}

    {{ return(query) }}
{% endmacro %}
