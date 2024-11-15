-- View containing values from each stage of assessment by PIN and year
-- in long format

-- List of types of columns that we will extract when pivoting vw_pin_value
-- from wide to long
{% set cols = [
    "class", "bldg", "land", "tot", "bldg_mv", "land_mv", "tot_mv"
] %}

-- Pivot vw_pin_value from wide to long, so that we create one row for each
-- assessment stage with values pulled from the column types we defined above
WITH stage_values AS (
    SELECT
        t1.pin,
        t1.year,
        t2.class,
        t2.stage_name,
        t2.stage_num,
        t2.bldg,
        t2.land,
        t2.tot,
        t2.bldg_mv,
        t2.land_mv,
        t2.tot_mv
    FROM {{ ref("default.vw_pin_value") }} AS t1
    CROSS JOIN
        UNNEST(
            ARRAY[
                'PRE-MAILED',
                'MAILED',
                'ASSESSOR PRE-CERTIFIED',
                'ASSESSOR CERTIFIED',
                -- This is a slightly different stage name from the name that
                -- we use in vw_pin_value ('BOARD CERTIFIED'). At some point
                -- we may want to align these names, but for now we maintain
                -- the difference for legacy compatibility
                'BOR CERTIFIED'
            ],
            ARRAY[0.5, 1, 1.5, 2, 3],
            {% for col in cols %}
                ARRAY[
                    pre_mailed_{{ col }},
                    mailed_{{ col }},
                    pre_certified_{{ col }},
                    certified_{{ col }},
                    board_{{ col }}
                ]
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
            AS t2 (
                stage_name,
                stage_num,
                {% for col in cols %}
                    {{ col }}{% if not loop.last %},{% endif %}
                {% endfor %}
            )
    -- Since we're enumerating the valid stage names by hand, rather than
    -- pulling them from the values that are present in the underlying
    -- vw_pin_value view, we need some way of determining when a PIN does not
    -- have a value for a particular stage in a given year. Checking for a null
    -- class is one way of doing that, since it means the `pre_{stage}_class`
    -- column is null, which should only be possible in cases where that stage
    -- doesn't exist for the PIN/year. Note that we test this assumption with
    -- a data test (`default_vw_pin_value_class_is_null_when_values_are_null`)
    -- to ensure that it doesn't change in the future
    WHERE t2.class IS NOT NULL
)

SELECT
    svls.pin,
    svls.year,
    svls.class,
    groups.reporting_class_code AS major_class,
    groups.modeling_group AS property_group,
    svls.stage_name,
    svls.stage_num,
    svls.bldg,
    svls.land,
    svls.tot,
    svls.bldg_mv,
    svls.land_mv,
    svls.tot_mv
FROM stage_values AS svls
-- Exclude classes without a reporting class
INNER JOIN {{ ref('ccao.class_dict') }} AS groups
    ON svls.class = groups.class_code
