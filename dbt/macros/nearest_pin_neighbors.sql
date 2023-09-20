-- Macro that takes a `source_model` containing PIN geometries and joins it
-- against spatial.parcel in order to generate the `num_neighbors` nearest
-- neighbors for each PIN for each year in the data, where a "neighbor" is
-- defined as another PIN that is within `radius_km` of the given PIN.
--
-- The `source_model` must contain four required columns, and is modeled
-- after spatial.parcel, which is the primary source for this macro:
--
-- * pin10
-- * year
-- * x_3435
-- * y_3435
{% macro nearest_pin_neighbors(source_model, num_neighbors, radius_km) %}
    with
        pin_locations as (
            select pin10, year, x_3435, y_3435, st_point(x_3435, y_3435) as point
            from {{ source("spatial", "parcel") }}
        ),

        most_recent_pins as (
            -- Parcel centroids may shift very slightly over time in GIS shapefiles.
            -- We want to make sure we only grab the most recent instance of a given
            -- parcel to avoid duplicates caused by these slight shifts.
            select
                x_3435,
                y_3435,
                pin10,
                rank() over (partition by pin10 order by year desc) as r
            from {{ source_model }}
        ),

        distinct_pins as (
            select distinct x_3435, y_3435, pin10 from most_recent_pins where r = 1
        ),

        pin_dists as (
            select *
            from
                (
                    select
                        dists.*,
                        row_number() over (
                            partition by dists.x_3435, dists.y_3435, dists.year
                            order by dists.dist
                        ) as row_num
                    from
                        (
                            select
                                dp.pin10,
                                dp.x_3435,
                                dp.y_3435,
                                loc.year,
                                loc.pin10 as neighbor_pin10,
                                st_distance(
                                    st_point(dp.x_3435, dp.y_3435), loc.point
                                ) as dist
                            from distinct_pins as dp
                            inner join
                                pin_locations as loc
                                on st_contains(
                                    st_buffer(
                                        st_point(dp.x_3435, dp.y_3435), {{ radius_km }}
                                    ),
                                    loc.point
                                )
                                and dp.pin10 != loc.pin10
                        ) as dists
                )
            where row_num <= 4
        )

    select *
    from
        (
            select
                pcl.pin10,
                {% for idx in range(1, num_neighbors + 1) %}
                    max(
                        case when pd.row_num = {{ idx + 1 }} then pd.neighbor_pin10 end
                    ) as nearest_neighbor_{{ idx }}_pin10,
                    max(
                        case when pd.row_num = {{ idx + 1 }} then pd.dist end
                    ) as nearest_neighbor_{{ idx }}_dist_ft,
                {% endfor %}
                pcl.year
            from {{ source("spatial", "parcel") }} as pcl
            inner join pin_dists as pd on pcl.pin10 = pd.pin10 and pcl.year = pd.year
            group by pcl.pin10, pcl.year
        )
    where
        {% for idx in range(1, num_neighbors + 1) %}
            {% if idx != 1 %} and{% endif %}
            nearest_neighbor_{{ idx }}_pin10 is not null
        {% endfor %}
{% endmacro %}
