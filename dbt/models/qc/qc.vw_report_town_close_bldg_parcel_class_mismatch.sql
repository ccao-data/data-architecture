{% set comparison_tables = ['comdat', 'dweldat', 'oby'] %}
{% for tablename in comparison_tables %}
    {% if loop.first %}WITH {% endif %}{{ tablename }}_mismatched_classes AS (
        SELECT
            pardat.parid,
            pardat.taxyr,
            ARRAY_JOIN(
                ARRAY_AGG(DISTINCT {{ tablename }}.class),
                ', '
            ) AS {{ tablename }}_classes
        FROM {{ source('iasworld', 'pardat') }} AS pardat
        LEFT JOIN {{ source('iasworld', tablename) }} AS {{ tablename }}
            ON pardat.parid = {{ tablename }}.parid
            AND pardat.taxyr = {{ tablename }}.taxyr
            AND {{ tablename }}.cur = 'Y'
            AND {{ tablename }}.deactivat IS NULL
        WHERE pardat.cur = 'Y'
            AND pardat.deactivat IS NULL
            AND pardat.class != {{ tablename }}.class
        GROUP BY pardat.parid, pardat.taxyr
    ){% if not loop.last %},{% endif %}
{% endfor %}

SELECT
    legdat.parid,
    legdat.taxyr,
    legdat.user1 AS township_code,
    pardat.class AS parcel_class,
{% for tablename in comparison_tables %}
    {{ tablename }}.{{ tablename }}_classes{% if not loop.last %},{% endif %}
{% endfor %}
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.parid = legdat.parid
    AND pardat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
{% for tablename in comparison_tables %}
    LEFT JOIN {{ tablename }}_mismatched_classes AS {{ tablename }}
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
    AND (
        {% for tablename in comparison_tables %}
            {{ tablename }}.{{ tablename }}_classes IS NOT NULL
            {% if not loop.last %}OR{% endif %}
        {% endfor %}
    )
