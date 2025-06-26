{% macro dist_to_nearest_geometry(source_model) %}

    with
        -- Unique parcel coordinates
        distinct_pins as (
            select distinct x_3435, y_3435 from {{ source("spatial", "parcel") }}
        ),

        -- All years in parcel data
        distinct_years as (select distinct year from {{ source("spatial", "parcel") }}),

        -- Forward-fill target years from source years
        fill_years as (
            select dy.year as pin_year, max(df.year) as fill_year
            from {{ source_model }} as df
            cross join distinct_years as dy
            where dy.year >= df.year
            group by dy.year
        ),

        -- Forward-filled geometry records
        location as (
            select fy.pin_year, fill_data.*
            from fill_years fy
            inner join {{ source_model }} fill_data on fy.fill_year = fill_data.year
        ),

        -- Collapse all geometries to single union per year
        location_agg as (
            select
                loc.pin_year,
                max(loc.year) as fill_year,
                geometry_union_agg(st_geomfrombinary(loc.geometry_3435)) as geom_3435
            from location loc
            group by loc.pin_year
        ),

        -- Get nearest points between each PIN and the aggregated geometry
        nearest_point as (
            select
                dp.x_3435,
                dp.y_3435,
                loc_agg.pin_year,
                geometry_nearest_points(
                    st_point(dp.x_3435, dp.y_3435), loc_agg.geom_3435
                ) as points
            from distinct_pins dp
            cross join location_agg loc_agg
        ),

        -- Single pass: join all locations, then apply fallback logic
        joined as (
            select
                np.x_3435,
                np.y_3435,
                loc.*,
                st_distance(np.points[1], np.points[2]) as dist_ft,
                -- Tag how it matched
                case
                    when
                        st_intersects(
                            np.points[2], st_geomfrombinary(loc.geometry_3435)
                        )
                    then 'intersects'
                    when st_equals(np.points[2], st_geomfrombinary(loc.geometry_3435))
                    then 'equals_fallback'
                    else null
                end as match_type
            from nearest_point np
            join
                location loc
                on abs(cast(np.pin_year as int) - cast(loc.pin_year as int)) = 0
            where
                -- Use fallback: intersects OR equals
                st_intersects(np.points[2], st_geomfrombinary(loc.geometry_3435))
                or st_equals(np.points[2], st_geomfrombinary(loc.geometry_3435))
        )

    -- Final output from unified logic
    select *
    from joined

{% endmacro %}
