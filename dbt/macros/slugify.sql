-- Variation of dbt_utils.slugify macro using kebab-case instead of snake_case
{% macro kebab_slugify(string) %}
    {#- Lower case the string -#}
    {% set string = string | lower %}

    {#- Replace spaces, slashes, and underscores with hyphens -#}
    {% set string = modules.re.sub("[ _/]+", "-", string) %}

    {#- Only take letters, numbers, and hyphens -#}
    {% set string = modules.re.sub("[^a-z0-9-]+", "", string) %}

    {#- Prepends "_" if string begins with a number -#}
    {% set string = modules.re.sub("^[0-9]", "_" + string[0], string) %}

    {{ return(string) }}
{% endmacro %}
