-- Macro that takes a model and returns its row count grouped and
-- sorted by year.
--
-- The name of the column containing year data can be specified with
-- the `year_column` argument, defaulting to "year". The sort order
-- can be specified with the `ordering` argument, defaulting to "asc".
--
-- If the `print` argument is set to True (default is False), the macro will
-- print the results of the query to stdout, which allows this macro to be used
-- by scripts to return data.
{% macro row_count_by_year(model, year_column="year", ordering="asc", print=False) %}
    {% set query %}
        select count(*) as COUNT, {{ year_column }}
        from {{ model }}
        group by {{ year_column }}
        order by {{ year_column }} {{ ordering }}
    {% endset %}

    {% if print %}
        {% set results = run_query(query) %} {{ results.print_json() }}
    {% endif %}

    {{ return(query) }}
{% endmacro %}
