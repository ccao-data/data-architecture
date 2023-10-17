{% macro nearest_neighbors(
    neighbor_model,
    neighbor_id_field,
    num_neighbors,
    radii_km,
    neighbor_model_is_point=False
) %}

    with
        most_recent_pins as (
            -- Parcel centroids may shift very slightly over time in GIS shapefiles.
            -- We want to make sure we only grab the most recent instance of a given
            -- parcel to avoid duplicates caused by these slight shifts.
            select
                x_3435,
                y_3435,
                pin10,
                rank() over (partition by pin10 order by year desc) as year_rank
            from {{ source("spatial", "parcel") }}
        ),

        distinct_pins as (
            select distinct x_3435, y_3435, pin10
            from most_recent_pins
            where year_rank = 1
        ),

        distinct_pins_with_years as (
            select dp.*, pcl.year
            from distinct_pins as dp
            inner join {{ source("spatial", "parcel") }} as pcl on dp.pin10 = pcl.pin10
        ),

        {% set pin_model = "distinct_pins_with_years" %}

        {% for radius_km in radii_km %}
            distances_{{ radius_km }} as (
                select *
                from
                    (
                        select
                            dists.*,
                            row_number() over (
                                partition by dists.x_3435, dists.y_3435, dists.year
                                order by dists.distance
                            ) as rank
                        from
                            (
                                select
                                    pcl.pin10,
                                    pcl.x_3435,
                                    pcl.y_3435,
                                    pcl.year,
                                    nghbr.{{ neighbor_id_field }} as neighbor_id,
                                    st_distance(
                                        st_point(pcl.x_3435, pcl.y_3435),
                                        {% if neighbor_model_is_point %}
                                            st_point(nghbr.x_3435, nghbr.y_3435)
                                        {% else %}
                                            st_geomfrombinary(nghbr.geometry_3435)
                                        {% endif %}
                                    ) as distance
                                from {{ pin_model }} as pcl
                                inner join
                                    {{ neighbor_model }} as nghbr
                                    {% if neighbor_model_is_point %}
                                        on st_contains(
                                            st_buffer(
                                                st_point(pcl.x_3435, pcl.y_3435),
                                                {{ radius_km }}
                                            ),
                                            st_point(nghbr.x_3435, nghbr.y_3435)
                                        )
                                    {% else %}
                                        on st_intersects(
                                            st_buffer(
                                                st_point(pcl.x_3435, pcl.y_3435),
                                                {{ radius_km }}
                                            ),
                                            st_geomfrombinary(nghbr.geometry_3435)
                                        )
                                    {% endif %} and pcl.year = nghbr.year
                                    {% if neighbor_model == source(
                                        "spatial", "parcel"
                                    ) %} and pcl.pin10 != nghbr.pin10 {% endif %}
                            ) as dists
                    )
                where rank <= {{ num_neighbors }}
            )

            {% if not loop.last %}
                -- Get a list of coords that are did not have a match in the
                -- previous nearest neighbors calculation, so that we can expand
                -- the radius and try again
                ,
                missing_matches_{{ radius_km }} as (
                    select pcl.*
                    from {{ pin_model }} as pcl
                    left join
                        distances_{{ radius_km }} as dist
                        on pcl.pin10 = dist.pin10
                        and pcl.year = dist.year
                    where dist.pin10 is null
                ),

                {% set pin_model = "missing_matches_{0}".format(radius_km) %}

            {% endif %}
        {% endfor %}

    {% for radius_km in radii_km %}
        {% if not loop.first %}
            union
        {% endif %}
        select *
        from distances_{{ radius_km }}
    {% endfor %}
{% endmacro %}
