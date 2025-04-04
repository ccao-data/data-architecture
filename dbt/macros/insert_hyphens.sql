-- fmt: off
--
-- Insert hyphens into a string.
--
-- Pass the index positions of the string where the hyphens should occur as one
-- or more positional arguments to this macro. The macro expects positions to
-- be 1-indexed, and expects the index to describe the position of the
-- character *after which* the hyphen should occur.
--
-- For example, to perform this string transformation:
--
--     "123456789" -> "12-34-567-89"
--
-- Call the macro like so:
--
--     insert_hyphens("'123456798'", 2, 4, 7)
--
-- Remember to wrap any identifiers in strings before passing them to the
-- macro, since otherwise the jinja parser won't be able to resolve the
-- reference. For example, if you wanted to transform a column named
-- `database.column`, you would call the macro like so:
--
--     insert_hyphens("database.column", 2, 4, 7)
--
-- fmt: on
{%- macro insert_hyphens(str) -%}
    {{ return(_insert_hyphens(str, varargs, exceptions.raise_compiler_error)) }}
{%- endmacro -%}

{%- macro _insert_hyphens(str, positions, raise_error_func) -%}
    {#- Validate arguments -#}
    {%- if str is not string -%}
        {{
            return(
                raise_error_func(
                    "insert_hyphens expects the first argument to be a string to insert hyphens into"
                )
            )
        }}
    {%- endif -%}
    {%- if not positions -%}
        {{
            return(
                raise_error_func(
                    "insert_hyphens expects one or more positional arguments"
                )
            )
        }}
    {%- endif -%}
    {%- for position in positions -%}
        {%- if position is not integer -%}
            {{
                return(
                    raise_error_func(
                        "insert_hyphens expects all positional arguments to be integers"
                    )
                )
            }}
        {%- endif -%}
    {%- endfor -%}

    {#-
        Start the positions at 0 to generalize the later logic that computes
        the position and length of each SUBSTR() call so that it works in the
        base case
    -#}
    {%- set positions = positions | list -%}
    {%- set positions = [0] + positions -%}

    concat(
        {%- for cur_pos in positions -%}
            {%- if not loop.last -%}
                {#-
                    If this is not the last position, then we need to know two
                    things:

                        1. The starting position of the current substring
                        2. The length of the current substring

                    These are the arguments to the SUBSTR() function.

                    The current starting position is always 1 + the current
                    _listed_ position (since each position in the list represents
                    the 1-indexed position of a character that precedes a hyphen,
                    and the 0 we prepended to the list has shifted every
                    position over). Once we know the starting position, then the
                    length of the current substring is always the next position
                    in the list minus the current starting position, since that's
                    the number of characters that takes us to the position of the
                    next hyphen
                -#}
                {%- set next_pos = positions[loop.index0 + 1] -%}
                {%- set cur_len = next_pos - cur_pos -%}
                substr({{ str }}, {{ cur_pos + 1 }}, {{ cur_len }}),  {#- this tag strips whitespace -#}
                '-',
            {%- else -%}
                {#-
                    If this _is the last position, then we only need to know the
                    starting position of the current substring, since SUBSTR()
                    will automatically take us to the end of the string if we
                    omit the second argument
                -#}
                substr({{ str }}, {{ cur_pos + 1 }})
            {%- endif -%}
        {%- endfor -%}
    )
{%- endmacro -%}
