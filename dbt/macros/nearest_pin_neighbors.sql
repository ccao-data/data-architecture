{% macro nearest_pin_neighbors(source_model, num_neighbors, radius_km) %}
    WITH pin_locations AS (
        SELECT
            pin10,
            year,
            x_3435,
            y_3435,
            ST_POINT(x_3435, y_3435) AS point
        FROM {{ source('spatial', 'parcel') }}
    ),

    most_recent_pins AS (
        -- Parcel centroids may shift very slightly over time in GIS shapefiles.
        -- We want to make sure we only grab the most recent instance of a given
        -- parcel to avoid duplicates caused by these slight shifts.
        SELECT
            x_3435,
            y_3435,
            RANK() OVER (PARTITION BY pin10 ORDER BY year DESC) AS r
        FROM {{ source_model }}
    ),

    distinct_pins AS (
        SELECT DISTINCT
            x_3435,
            y_3435
        FROM most_recent_pins
        WHERE r = 1
    ),

    pin_dists AS (
        SELECT *
        FROM (
            SELECT
                dists.*,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY dists.x_3435, dists.y_3435, dists.year
                        ORDER BY dists.dist
                    )
                    AS row_num
            FROM (
                SELECT
                    dp.x_3435,
                    dp.y_3435,
                    loc.year,
                    loc.pin10,
                    ST_DISTANCE(ST_POINT(dp.x_3435, dp.y_3435), loc.point) AS dist
                FROM distinct_pins AS dp
                INNER JOIN pin_locations AS loc
                    ON ST_CONTAINS(
                        ST_BUFFER(
                            ST_POINT(
                                dp.x_3435,
                                dp.y_3435
                            ),
                            {{ radius_km }}
                        ),
                        loc.point
                    )
            ) AS dists
        )
        WHERE row_num <= 4
    )

    SELECT *
    FROM (
        SELECT
            pcl.pin10,
            {% for idx in range(1, num_neighbors + 1) %}
            MAX(CASE
                WHEN pd.row_num = {{ idx + 1 }} THEN pd.pin10
            END) AS nearest_neighbor_{{ idx }}_pin10,
            MAX(CASE
                WHEN pd.row_num = {{ idx + 1 }} THEN pd.dist
            END) AS nearest_neighbor_{{ idx }}_dist_ft,
            {% endfor %}
            pcl.year
        FROM {{ source('spatial', 'parcel') }} AS pcl
        INNER JOIN pin_dists AS pd
            ON pcl.x_3435 = pd.x_3435
            AND pcl.y_3435 = pd.y_3435
            AND pcl.year = pd.year
        GROUP BY pcl.pin10, pcl.year
    )
    WHERE
    {% for idx in range(1, num_neighbors + 1) %}
        {% if idx != 1 %}AND{% endif %}
        nearest_neighbor_{{ idx }}_pin10 IS NOT NULL
    {% endfor %}
{% endmacro %}
