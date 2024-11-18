{% set stages = ['board', 'certified', 'mailed'] %}

WITH nulls AS (
    SELECT
        pin,
        year,
        stage_name,
    {% for stage in stages %}
        {{ stage }}_class IS NULL AS {{ stage }}_class_is_null,
        {{ stage }}_tot_mv IS NULL AS {{ stage }}_tot_mv_is_null,
        {{ stage }}_tot IS NULL AS {{ stage }}_tot_is_null{% if not loop.last %}

            ,
        {% endif %}
    {% endfor %}
    FROM {{ ref("default.vw_pin_value") }}
)

SELECT *
FROM nulls
WHERE
{% for stage in stages %}
    {{ stage }}_class_is_null
    OR {{ stage }}_tot_mv_is_null
    OR {{ stage }}_tot_is_null{% if not loop.last %} OR{% endif %}
{% endfor %}
