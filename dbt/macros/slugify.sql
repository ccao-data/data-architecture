-- Variation of dbt_utils.slugify macro using strict snake_case
{% macro slugify(string) %}
    {#- Lower case the string -#}
    {% set string = string | lower %}

    {#- Replace spaces, slashes, and hyphens with underscores -#}
    {% set string = modules.re.sub("[ -/]+", "_", string) %}

    {#- Only take letters, numbers, and hyphens -#}
    {% set string = modules.re.sub("[^a-z0-9_]+", "", string) %}

    {{ return(string) }}
{% endmacro %}
