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

        source_pins as (
            select pin10, year, x_3435, y_3435, st_point(x_3435, y_3435) as point
            from {{ source_model }}
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
                                sp.pin10,
                                sp.x_3435,
                                sp.y_3435,
                                loc.year,
                                loc.pin10 as neighbor_pin10,
                                st_distance(
                                    st_point(sp.x_3435, sp.y_3435), loc.point
                                ) as dist
                            from source_pins as sp
                            inner join
                                pin_locations as loc
                                on st_contains(
                                    st_buffer(sp.point, {{ radius_km }}), loc.point
                                )
                            -- This horrifying conditional is designed to trick the
                            -- Athena query planner. For some reason, adding a true
                            -- conditional to a query with a spatial join (like the
                            -- one above) results in terrible performance, while doing
                            -- a cross join then filtering the rows is much faster
                            where abs(cast(loc.year as int) - cast(sp.year as int)) = 0
                        ) as dists
                )
            where row_num <= {{ num_neighbors }} + 1
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
            {% if idx != 1 %} and {% endif %}
            nearest_neighbor_{{ idx }}_pin10 is not null
        {% endfor %}
{% endmacro %}
