-- Macro that takes a `source_model` containing geometries and joins it
-- against spatial.parcel in order to generate the distance from each PIN
-- to each geometry and year combination.
{% macro dist_to_nearest_geometry(source_model) %}

    with
        distinct_pins as (
            select distinct x_3435, y_3435 from {{ source("spatial", "parcel") }}
        ),

        distinct_years as (select distinct year from {{ source("spatial", "parcel") }}),

        location as (
            select fill_years.pin_year, fill_data.*
            from
                (
                    select dy.year as pin_year, max(df.year) as fill_year
                    from {{ source_model }} as df
                    cross join distinct_years as dy
                    where dy.year >= df.year
                    group by dy.year
                ) as fill_years
            left join
                {{ source_model }} as fill_data on fill_years.fill_year = fill_data.year
        ),

        location_agg as (
            select
                dy.year as pin_year,
                max(df.year) as fill_year,
                geometry_union_agg(st_geomfrombinary(df.geometry_3435)) as geom_3435
            from {{ source_model }} as df
            cross join distinct_years as dy
            where dy.year >= df.year
            group by dy.year
        ),

        nearest_point as (
            select
                dp.x_3435,
                dp.y_3435,
                geometry_nearest_points(
                    st_point(dp.x_3435, dp.y_3435), loc_agg.geom_3435
                ) as points
            from distinct_pins as dp
            cross join location_agg as loc_agg
        )

    select
        np.x_3435, np.y_3435, loc.*, st_distance(np.points[1], np.points[2]) as dist_ft
    from nearest_point as np
    left join
        location as loc
        on st_intersects(np.points[2], st_geomfrombinary(loc.geometry_3435))
{% endmacro %}
