{% macro nearest_feature_aggregation(base_columns, characteristics) %}
    {%- for characteristic in characteristics %}
        case
            {%- for base in base_columns %}
                when {{ base }} = least({{ base_columns | join(", ") }})
                then {{ base.replace("dist_ft", characteristic) }}
            {%- endfor %}
            else null
        end as closest_{{ characteristic }},
    {%- endfor %}
{% endmacro %}
