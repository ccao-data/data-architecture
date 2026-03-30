-- Macro that cleans and concatenates address columns by replacing multiple
-- spaces with a single space, removing null characters, and returning null
-- if the result is an empty string.
{% macro concat_address(address_columns) %}
    {% set column_name = address_columns | join(", ") %}
    nullif(
        trim(
            regexp_replace(
                regexp_replace(concat_ws(' ', {{ column_name }}), '[[:cntrl:]]', ''),
                '\s+',
                ' '
            )
        ),
        ''
    )
{% endmacro %}
