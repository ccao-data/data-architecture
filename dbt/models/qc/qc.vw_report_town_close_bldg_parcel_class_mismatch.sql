{% set comparison_tables = ['comdat', 'dweldat', 'oby'] %}

SELECT
    legdat.parid,
    legdat.taxyr,
    legdat.user1 AS township_code,
    pardat.class AS parcel_class,
{% for tablename in comparison_tables %}
    ARRAY_JOIN({{ tablename }}.classes, ', ')
        AS {{ tablename }}_classes{% if not loop.last %}, {% endif %}
{% endfor %}
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.parid = legdat.parid
    AND pardat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
{% for tablename in comparison_tables %}
    LEFT JOIN (
        SELECT
            {{ tablename }}.parid,
            {{ tablename }}.taxyr,
            -- If the comparison table only has rows for this PIN/year with
            -- null class codes, a naive `ARRAY_AGG()` will return an array
            -- with one null element, which causes the `CONTAINS()` check below
            -- to return null and so exclude the row from the result set (even
            -- though this should count as a mismatch). To handle this edge
            -- case, ignore nulls when building the array of distinct classes,
            -- and format any resulting empty arrays as ['NULL'] so that they
            -- will always fail the `CONTAINS()` check below while also
            -- returning output that the `ARRAY_JOIN()` call in the `SELECT`
            -- clause above can turn into useful, human-readable output
            COALESCE(
                ARRAY_AGG(DISTINCT {{ tablename }}.class) FILTER (
                    WHERE {{ tablename }}.class IS NOT NULL
                ),
                ARRAY['NULL']
            ) AS classes
        FROM {{ source('iasworld', tablename) }} AS {{ tablename }}
        WHERE {{ tablename }}.cur = 'Y'
            AND {{ tablename }}.deactivat IS NULL
        GROUP BY {{ tablename }}.parid, {{ tablename }}.taxyr
    ) AS {{ tablename }}
        ON pardat.parid = {{ tablename }}.parid
        AND pardat.taxyr = {{ tablename }}.taxyr
{% endfor %}
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND pardat.class NOT IN (
        '100', '200', '239', '240', '241', '300', '400', '500', '535', '550'
    )
    AND SUBSTR(pardat.class, 1, 3) NOT IN (
        '637', '700', '742', '800', '900'
    )
    -- Parcels fail the check if one of the comparison tables has classes
    -- that A) exist (aka are not null) and B) have no match to the
    -- parcel class
    AND (
        {% for tablename in comparison_tables %}
            (
                -- Only run this check if the column is not null, since
                -- otherwise CONTAINS() will return null as well
                {{ tablename }}.classes IS NULL
                OR NOT CONTAINS({{ tablename }}.classes, pardat.class)
            )
            {% if not loop.last %}AND{% endif %}
        {% endfor %}
    )
    -- Check for the edge case where a parcel is exempt and none of the
    -- comparison tables exist, which should not be returned but can pass
    -- the conditional above due to all table joins returning nulls
    AND NOT (
        pardat.class IN ('EX', 'RR')
        AND
        {% for tablename in comparison_tables %}
            {{ tablename }}.classes IS NULL
            {% if not loop.last %}AND{% endif %}
        {% endfor %}

    )
