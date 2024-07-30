{% set comparison_tables = ['comdat', 'dweldat', 'oby'] %}
{% for tablename in comparison_tables %}
    {% if loop.first %}WITH {% endif %}{{ tablename }}_mismatched_classes AS (
        SELECT
            pardat.parid,
            pardat.taxyr,
            ARRAY_JOIN({{ tablename }}.classes, ', ') AS classes
        FROM {{ source('iasworld', 'pardat' ) }} AS pardat
        LEFT JOIN (
            SELECT
                {{ tablename }}.parid,
                {{ tablename }}.taxyr,
                ARRAY_AGG(DISTINCT {{ tablename }}.class) AS classes
            FROM {{ source('iasworld', tablename) }} AS {{ tablename }}
            WHERE {{ tablename }}.cur = 'Y'
                AND {{ tablename }}.deactivat IS NULL
            GROUP BY {{ tablename }}.parid, {{ tablename }}.taxyr
        ) AS {{ tablename }}
            ON pardat.parid = {{ tablename }}.parid
            AND pardat.taxyr = {{ tablename }}.taxyr
        WHERE NOT CONTAINS({{ tablename }}.classes, pardat.class)
    ){% if not loop.last %},{% endif %}
{% endfor %}

SELECT
    legdat.parid,
    legdat.taxyr,
    legdat.user1 AS township_code,
    pardat.class AS parcel_class,
{% for tablename in comparison_tables %}
    {{ tablename }}.classes AS {{ tablename }}_classes{% if not loop.last %}
        ,
    {% endif %}
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
            {{ tablename }}.classes IS NOT NULL
            {% if not loop.last %}OR{% endif %}
        {% endfor %}
    )
