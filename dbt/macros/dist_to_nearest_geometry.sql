-- Macro that takes a `source_model` containing geometries and joins it
-- against `spatial.parcel` in order to generate the distance from each PIN
-- to each geometry and year combination
{% macro dist_to_nearest_geometry(source_model, geometry_type="polygon") %}

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
        -- Each year of the `source_model` needs to be a complete set of observations. 
        -- For example, if a park is constructed in 2020, all parks from prior years 
        -- need to be included in the 2020 data.
        -- The `source_model` needs to have a comparable year in spatial.parcel to
        -- join (>= 2000).
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

        -- Source table with forward filling applied by year. This
        -- will result in one row per geometry per year of parcel data i.e.
        -- for hospitals, there will be one row for each hospital for each
        -- year of parcel data (up to the current year)
        location as (
            select fy.pin_year, fill_data.*
            from fill_years as fy
            inner join {{ source_model }} as fill_data on fy.fill_year = fill_data.year
        ),

        -- Source table with forward filling applied by year, but containing
        -- the union of all geometries for each year. This will result in just
        -- one record per year, with all geometries for each year squished into
        -- a single object
        location_agg as (
            select
                loc.pin_year,
                max(loc.year) as fill_year,
                geometry_union_agg(st_geomfrombinary(loc.geometry_3435)) as geom_3435
            from location as loc
            group by loc.pin_year
        ),

        -- For each unique PIN location, find the nearest point from each
        -- geometry set from each year. The output of geometry_nearest_points
        -- is a pair of points, one from each geometry and year
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

    -- Using the nearest point from each target geometry and year, join the
    -- nearest location data (name, id, etc.) to each PIN. Also calculate
    -- distance between the nearest points
    select
        np.x_3435, np.y_3435, loc.*, st_distance(np.points[1], np.points[2]) as dist_ft
    from nearest_point as np
    inner join
        location as loc
        -- This conditional fixes a floating point error that can occur when
        -- aggregating the geometries of a large number of points that are
        -- very close together. In these cases, `geometry_union_agg()` can
        -- merge multiple close points into one, which causes a problem when
        -- we try to join points back to the source data, since they will have
        -- changed slightly due to the aggregation.
        --
        -- We handle this case by rounding both the target coordinates and the
        -- comparison coordinates to 2 decimal places, ensuring that they join
        -- even if they have been consolidated during aggregation. We consider
        -- 2 decimal places to be an acceptable loss of resolution because
        -- these geometries are in units of linear feet, and we don't actually
        -- have parcel coordinates at a resolution higher than 100ths of a
        -- foot.
        --
        -- Note that the point consolidation and our rounding mean that it is
        -- possible that we will join to different parcels that are very close
        -- together on subsequent runs. This should be fine, since we only
        -- really care the distance, and parcel distances should be identical
        -- at the scale of 100ths of a foot. In order to deduplicate parcels
        -- with similar locations, make sure to group outputs and select columns
        -- using ARBITRARY aggregation when calling this macro.
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
    -- This horrifying conditional is designed to trick the Athena query
    -- planner. For some reason, adding a true conditional to a query with a
    -- spatial join (like the one above) results in terrible performance,
    -- while doing a cross join then filtering the rows is much faster
    where abs(cast(np.pin_year as int) - cast(loc.pin_year as int)) = 0
{% endmacro %}
