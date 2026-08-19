{#-
    Generate a QC report view that runs a set of boolean "tests" against a
    base query and returns one row per (record, failing test) pair. We use
    the resulting views to power iasWorld QC.

    Args:
        base_query: A SQL SELECT statement (as a string) that returns the
            identifying columns plus any additional columns referenced by
            the `condition` or `additional_select_columns` attributes of the
            `tests` argument. Source tables that don't include a required
            identifying column (usually `card` or `lline`) should return
            `CAST(NULL AS <type>) AS <column_name>` for that column to conform
            to the expected shape. Required identifying columns include:
                - parid: varchar
                - taxyr: varchar
                - card: decimal
                - lline: decimal
                - township_code: varchar
                - class: varchar
                - who: varchar
                - wen: varchar
        tests: A list of dicts, each with keys:
            - name: A unique, descriptive slug for the test
            - description: A human-readable description of what the test checks
            - category: A category slug used to group related tests
            - condition: A SQL boolean expression, evaluated against
                `base_query`, that is TRUE when the record passes the test
                and FALSE when it fails
            - additional_select_columns (optional): A list of column names
                from `base_query` to include in the output as a map of column
                name -> stringified value for records that fail the test

    Returns:
        A query that selects one row per record per failing test.
-#}
{% macro generate_iasworld_qc_test_view(base_query, tests) %}
    {% do _validate_iasworld_qc_tests(tests, exceptions.raise_compiler_error) %}
    with
        base as ({{ base_query }}),

        test_result as (
            select
                *,
                -- noqa: disable=layout.indent
                {% for test in tests %}
                    not ({{ test.condition }}) as {{ test.name }}
                    {{- "," if not loop.last }}
                {% endfor %}
            -- noqa: enable=layout.indent
            from base
            where
                taxyr
                between '{{ var("data_test_iasworld_year_start") }}'
                and '{{ var("data_test_iasworld_year_end") }}'
        )

    {% for test in tests %}
        select
            parid,
            taxyr,
            card,
            lline,
            township_code,
            class,
            who,
            wen,
            '{{ test.name }}' as test_name,
            '{{ test.description }}' as test_description,
            '{{ test.category }}' as test_category,
            {% if test.additional_select_columns -%}
                map(
                    array[
                        {%- for col_name in test.additional_select_columns -%}
                            '{{ col_name }}'{{ ", " if not loop.last }}
                        {%- endfor %}
                    ],
                    array[
                        {%- for col_name in test.additional_select_columns -%}
                            cast({{ col_name }} as varchar) {{- ", " if not loop.last }}
                        {%- endfor %}
                    ]
                ) as additional_columns
            {%- else -%} cast(null as map(varchar, varchar)) as additional_columns
            {%- endif %}
        from test_result
        where {{ test.name }} {{ "UNION ALL" if not loop.last }}
    {% endfor %}
{% endmacro %}

{#-
    Validate that `tests` is a list of dicts, each containing the keys
    required by `generate_iasworld_qc_test_view`. Raises a compiler error
    (via `raise_error_func`) on the first validation failure it finds.

    Args:
        tests: The `tests` argument passed to `generate_iasworld_qc_test_view`
        raise_error_func: A function to call with an error message when
            validation fails. Takes `exceptions.raise_compiler_error` in
            production, and a mock in unit tests so that the error can be
            returned for equality comparison instead of raised
-#}
{% macro _validate_iasworld_qc_tests(tests, raise_error_func) %}
    {%- set required_keys = ["name", "description", "category", "condition"] -%}
    {%- if tests is not iterable or tests is mapping or tests is string -%}
        {{-
            return(
                raise_error_func('"tests" argument must be a list, got: ' ~ tests)
            )
        -}}
    {%- endif -%}
    {%- for test in tests -%}
        {%- if test is not mapping -%}
            {{-
                return(
                    raise_error_func(
                        'Each element of "tests" must be an object/dict,'
                        ~ " got: "
                        ~ test
                    )
                )
            -}}
        {%- endif -%}
        {%- for key in required_keys -%}
            {%- if key not in test -%}
                {{-
                    return(
                        raise_error_func(
                            'Missing required "'
                            ~ key
                            ~ '" key in test'
                            ~ " config: "
                            ~ test
                        )
                    )
                -}}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {{ return(none) }}
{% endmacro %}
