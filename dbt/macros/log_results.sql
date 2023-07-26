{% macro log_results(results) %}

    {% if execute %}
  {{ log("", info=True) }}
  {{ log("========== Begin Summary ==========", info=True) }}
  {% for res in results -%}
    {% set line -%}
      node: {{ res.node.unique_id }}; status: {{ res.status }} in {{ res.execution_time|round(2) }}s (message: {{ res.message }})
    {%- endset %}

    {{ log(line, info=True) }}
  {% endfor %}
  {{ log("========== End Summary ==========", info=True) }}
  {{ log("", info=True) }}
  {% endif %}

{% endmacro %}
