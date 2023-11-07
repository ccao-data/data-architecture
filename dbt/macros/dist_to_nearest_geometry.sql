-- Macro that takes a `source_model` containing geometries and joins it
-- against `spatial.parcel` in order to generate the distance from each PIN
-- to each geometry and year combination
--
-- The `source_conditional` allows for filtering on the source table
-- using standard SQL, passed as a string
{% macro dist_to_nearest_geometry(source_model, source_conditional) %}

    with
        -- Source table with conditional applied (if applicable)
        source_table as (
            select * from {{ source_model }}
            {% if source_conditional is defined %}
                where {{ source_conditional }}
            {% endif %}
        ),
        
        -- Universe of all possible PINs. This ignores years since PINs don't
        -- actually move very often, so unique xy coordinates are a good enough
        -- proxy for PIN coordinates and year
        distinct_pins as (
            select distinct x_3435, y_3435 from {{ source("spatial", "parcel") }}
        ),

        -- Years that exist for parcel data. This determines the set of years
        -- for which data will be filled forward in time i.e. if park locations
        -- exist for 2020, and distinct_years goes up to 2023, then park data
        -- from 2020 will be filled forward to 2023
        distinct_years as (select distinct year from {{ source("spatial", "parcel") }}),

        -- Source table with forward filling applied by year. See above. This
        -- will result in one row per geometry per year of parcel data i.e.
        -- for hospitals, there will be one row for each hospital for each
        -- year of parcel data (up to the current year)
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
            left join source_table as fill_data on fill_years.fill_year = fill_data.year
        ),

        -- Source table with forward filling applied by year, but containing
        -- the union of all geometries for each year. This will result in just
        -- one record per year, with all geometries for each year squished into
        -- a single object
        location_agg as (
            select
                dy.year as pin_year,
                max(df.year) as fill_year,
                geometry_union_agg(st_geomfrombinary(df.geometry_3435)) as geom_3435
            from (
            ) as df
            cross join distinct_years as dy
            where dy.year >= df.year
            group by dy.year
        ),

        -- For each unique PIN location, find the nearest point from each
        -- geometry set from each year. The output of geometry_nearest_points
        -- is a pair of points, one from each geometry
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

    -- Using the nearest point from each target geometry and year, join the
    -- nearest location data (name, id, etc.) to each PIN. Also calculate
    -- distance between the nearest points
    select
        np.x_3435, np.y_3435, loc.*, st_distance(np.points[1], np.points[2]) as dist_ft
    from nearest_point as np
    left join
        location as loc
        on st_intersects(np.points[2], st_geomfrombinary(loc.geometry_3435))
{% endmacro %}
