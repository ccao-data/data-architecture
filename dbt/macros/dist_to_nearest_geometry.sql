{% macro dist_to_nearest_geometry(source_model) %}

    with
        -- Unique parcel coordinates
        distinct_pins as (
            select distinct x_3435, y_3435 from {{ source("spatial", "parcel") }}
        ),

        -- All parcel years
        distinct_years as (select distinct year from {{ source("spatial", "parcel") }}),

        -- Forward-fill mapping
        fill_years as (
            select dy.year as pin_year, max(df.year) as fill_year
            from {{ source_model }} as df
            cross join distinct_years as dy
            where dy.year >= df.year
            group by dy.year
        ),

        -- Forward-filled source data
        location as (
            select fy.pin_year, fill_data.*
            from fill_years as fy
            inner join {{ source_model }} as fill_data on fy.fill_year = fill_data.year
        ),

        -- Aggregated geometry union by year
        location_agg as (
            select
                loc.pin_year,
                max(loc.year) as fill_year,
                geometry_union_agg(st_geomfrombinary(loc.geometry_3435)) as geom_3435
            from location as loc
            group by loc.pin_year
        ),

        -- Find nearest points from parcels to geometry union
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
        ),

        -- Attempt fast spatial join using ST_Intersects
        intersects_join as (
            select
                np.x_3435,
                np.y_3435,
                loc.*,
                st_distance(np.points[1], np.points[2]) as dist_ft
            from nearest_point np
            join
                location loc
                on st_intersects(np.points[2], st_geomfrombinary(loc.geometry_3435))
            where abs(cast(np.pin_year as int) - cast(loc.pin_year as int)) = 0
        ),

        -- Only fallback to ST_Equals where ST_Intersects did not return matches
        equals_join as (
            select
                np.x_3435,
                np.y_3435,
                loc.*,
                st_distance(np.points[1], np.points[2]) as dist_ft
            from nearest_point np
            join
                location loc
                on st_equals(np.points[2], st_geomfrombinary(loc.geometry_3435))
            where
                abs(cast(np.pin_year as int) - cast(loc.pin_year as int)) = 0
                and not exists (
                    select 1
                    from intersects_join ij
                    where ij.x_3435 = np.x_3435 and ij.y_3435 = np.y_3435
                )
        )

    -- Combine results
    select *
    from intersects_join
    union all
    select *
    from equals_join

{% endmacro %}
