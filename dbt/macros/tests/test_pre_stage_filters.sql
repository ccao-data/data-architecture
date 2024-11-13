{% macro test_pre_stage_filters() %}
    {% do test_pre_stage_filters_params() %}
    {% do test_pre_mailed_filters() %}
    {% do test_pre_certified_filters() %}
{% endmacro %}

{% macro expected_pre_mailed_filters() %}
    asmt.procname is null
    and asmt.cur = 'Y'
    and not contains(stages.procnames, 'CCAOVALUE')
{% endmacro %}

{% macro expected_pre_certified_filters() %}
    asmt.procname is null
    and asmt.cur = 'Y'
    and not contains(stages.procnames, 'CCAOFINAL')
{% endmacro %}

{% macro test_pre_stage_filters_params() %}
    {{
        assert_equals(
            "test_pre_stage_filters_params",
            pre_stage_filters("asmt", "CCAOVALUE"),
            expected_pre_mailed_filters(),
        )
    }}
{% endmacro %}

{% macro test_pre_mailed_filters() %}
    {{
        assert_equals(
            "test_pre_mailed_filters_params",
            pre_mailed_filters("asmt"),
            expected_pre_mailed_filters(),
        )
    }}
{% endmacro %}

{% macro test_pre_certified_filters() %}
    {{
        assert_equals(
            "test_pre_certified_filters_params",
            pre_certified_filters("asmt"),
            expected_pre_certified_filters(),
        )
    }}
{% endmacro %}
