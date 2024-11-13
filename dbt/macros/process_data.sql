{% macro process_partitioned_data(partitions) %}
    {% set union_all_parts = [] %}

    with
        distinct_pins as (
            select distinct x_3435, y_3435, pin10 from {{ source("spatial", "parcel") }}
        )

        {% for partition in partitions %}
            ,
            nearest_for_{{ partition }} as (
                select
                    pcl.pin10,
                    xy.year,
                    xy.road_name as nearest_local_road_name,
                    xy.dist_ft as nearest_local_road_dist_ft,
                    xy.year as nearest_local_road_data_year,
                    xy.daily_traffic as nearest_local_road_daily_traffic,
                    xy.speed_limit as nearest_local_road_speed_limit,
                    xy.surface_type as nearest_local_road_surface_type,
                    xy.lanes as nearest_local_road_lanes,
                    row_number() over (
                        partition by pcl.pin10, xy.year order by xy.dist_ft
                    ) as row_num
                from distinct_pins as pcl
                inner join
                    {{ dist_to_nearest_geometry(ref(partition)) }} as xy
                    on pcl.x_3435 = xy.x_3435
                    and pcl.y_3435 = xy.y_3435
            )

            {% do union_all_parts.append("nearest_for_" ~ partition) %}
        {% endfor %},
        ranked_nearest_local as (
            select * from {{ union_all_parts | join(" union all\n") }}
        ),
        nearest_local as (
            select
                pin10,
                nearest_local_road_name,
                nearest_local_road_dist_ft,
                nearest_local_road_data_year,
                nearest_local_road_daily_traffic,
                nearest_local_road_speed_limit,
                nearest_local_road_surface_type,
                nearest_local_road_lanes,
                year
            from ranked_nearest_local
            where row_num = 1
        )

    select *
    from nearest_local
{% endmacro %}
