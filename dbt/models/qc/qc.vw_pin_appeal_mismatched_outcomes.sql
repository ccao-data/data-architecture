SELECT
    pin,
    year,
    case_no,
    change,
    mailed_bldg,
    certified_bldg,
    mailed_land,
    certified_land,
    mailed_tot,
    certified_tot
FROM {{ ref("default.vw_pin_appeal") }}
WHERE
    {% set vars = [
        ('mailed_bldg', 'certified_bldg'),
        ('mailed_land', 'certified_land'),
        ('mailed_tot', 'certified_tot'),
    ] %}
    (
        change = 'no change' AND (
        -- Use ABS(...) <= 1 instead of equality for comparison to account
        -- for the fact that values are not always rounded consistently
            {% for mailed_var, certified_var in vars %}
                {% if loop.index0 > 0 %}OR{% endif %}
                (
                    {{ mailed_var }} IS NOT NULL
                    AND {{ certified_var }} IS NOT NULL
                    AND (ABS({{ mailed_var }} - {{ certified_var }}) > 1)
                )
            {% endfor %}
        )
    )
    OR
    (
        change = 'change'
        {% for mailed_var, certified_var in vars %}
            AND (
                {{ mailed_var }} IS NOT NULL AND {{ certified_var }} IS NOT NULL
                AND (ABS({{ mailed_var }} - {{ certified_var }}) <= 1)
            )
        {% endfor %}
    )
