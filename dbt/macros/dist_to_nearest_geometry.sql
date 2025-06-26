{% macro dist_to_nearest_geometry(source_model, point_rounding=False) %}

    with
        -- Universe of all possible PINs. This ignores years since PINs don't
        -- actually move very often, so unique xy coordinates are a good enough
        -- proxy for PIN coordinates and year. We do this to limit the number
        -- of parcels for which we need to perform spatial operations
        distinct_pins as (
            select distinct x_3435, y_3435 from {{ source("spatial", "parcel") }}
        ),

        -- Years that exist for parcel data. This determines the set of years
        -- for which data will be filled forward in time i.e. if park locations
        -- exist for 2020, and distinct_years goes up to 2023, then park data
        -- from 2020 will be filled forward to 2023
        distinct_years as (select distinct year from {{ source("spatial", "parcel") }}),

        -- Crosswalk of the source data and distinct years, used to perform
        -- the forward filling described above
        fill_years as (
            select dy.year as pin_year, max(df.year) as fill_year
            from {{ source_model }} as df
            cross join distinct_years as dy
            where dy.year >= df.year
            group by dy.year
        ),

        -- Forward-filled source table by year
        location as (
            select fy.pin_year, fill_data.*
            from fill_years as fy
            inner join {{ source_model }} as fill_data on fy.fill_year = fill_data.year
        ),

        -- Geometry union per year for finding nearest points
        location_agg as (
            select
                loc.pin_year,
                max(loc.year) as fill_year,
                geometry_union_agg(st_geomfrombinary(loc.geometry_3435)) as geom_3435
            from location as loc
            group by loc.pin_year
        ),

        -- For each PIN location, find nearest geometry point for each year
        nearest_point as (
            select
                dp.x_3435,
                dp.y_3435,
                loc_agg.pin_year,
                geometry_nearest_points(
                    st_point(dp.x_3435, dp.y_3435), loc_agg.geom_3435
                ) as points
            from distinct_pins as dp
            cross join location_agg as loc_agg
        )

    -- Join back to individual location geometries to get metadata
    select
        np.x_3435, np.y_3435, loc.*, st_distance(np.points[1], np.points[2]) as dist_ft
    from nearest_point as np
    inner join
        location as loc
        on {% if point_rounding %}
            st_intersects(
                st_point(round(st_x(np.points[2]), 2), round(st_y(np.points[2]), 2)),
                st_point(
                    round(st_x(st_geomfrombinary(loc.geometry_3435)), 2),
                    round(st_y(st_geomfrombinary(loc.geometry_3435)), 2)
                )
            )
        {% else %} st_intersects(np.points[2], st_geomfrombinary(loc.geometry_3435))
        {% endif %}
    where abs(cast(np.pin_year as int) - cast(loc.pin_year as int)) = 0

{% endmacro %}
