{% macro test_generate_iasworld_qc_test_view() %}
    {% do test_validate_iasworld_qc_tests_not_a_list() %}
    {% do test_validate_iasworld_qc_tests_element_not_a_dict() %}
    {% do test_validate_iasworld_qc_tests_missing_required_key() %}
    {% do test_validate_iasworld_qc_tests_valid() %}
{% endmacro %}

{% macro test_validate_iasworld_qc_tests_not_a_list() %}
    {{
        assert_equals(
            "test_validate_iasworld_qc_tests_not_a_list",
            _validate_iasworld_qc_tests({"name": "foo"}, mock_raise_compiler_error),
            "\"tests\" argument must be a list, got: {'name': 'foo'}",
        )
    }}
{% endmacro %}

{% macro test_validate_iasworld_qc_tests_element_not_a_dict() %}
    {{
        assert_equals(
            "test_validate_iasworld_qc_tests_element_not_a_dict",
            _validate_iasworld_qc_tests(["foo"], mock_raise_compiler_error),
            'Each element of "tests" must be an object/dict, got: foo',
        )
    }}
{% endmacro %}

{% macro test_validate_iasworld_qc_tests_missing_required_key() %}
    {{
        assert_equals(
            "test_validate_iasworld_qc_tests_missing_required_key",
            _validate_iasworld_qc_tests(
                [
                    {
                        "name": "foo",
                        "description": "bar",
                        "category": "baz",
                    }
                ],
                mock_raise_compiler_error,
            ),
            "Missing required \"condition\" key in test config: {'name':"
            ~ " 'foo', 'description': 'bar', 'category': 'baz'}",
        )
    }}
{% endmacro %}

{% macro test_validate_iasworld_qc_tests_valid() %}
    {{
        assert_equals(
            "test_validate_iasworld_qc_tests_valid",
            _validate_iasworld_qc_tests(
                [
                    {
                        "name": "foo",
                        "description": "bar",
                        "category": "baz",
                        "condition": "1 = 1",
                    }
                ],
                mock_raise_compiler_error,
            ),
            none,
        )
    }}
{% endmacro %}
