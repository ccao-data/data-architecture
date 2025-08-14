library("ccao")
library("dplyr")
library("geoarrow")
library("mapview")
library("sf")
library("stringr")

towns <- ccao::town_shp %>%
  st_transform(3435)

munis <- read_geoparquet_sf(
  paste0(
    "s3://ccao-data-warehouse-us-east-1/spatial/political/municipality/",
    "year=2024/part-0.parquet"
  )
) %>%
  st_transform(3435) %>%
  mutate(
    geo_type = "municipality",
    geo_num = as.character(municipality_num)
  ) %>%
  select(
    geo_type,
    geo_name = municipality_name,
    geo_num,
    geometry, geometry_3435
  )

unincorporated <- muni %>%
  filter(municipality_name == "Unincorporated") %>%
  st_join(towns)

city <- read_geoparquet_sf(
  paste0(
    "s3://ccao-data-warehouse-us-east-1/spatial/other/community_area/",
    "year=2018/part-0.parquet"
  )
) %>%
  st_transform(3435) %>%
  mutate(
    geo_type = "community area",
    geo_name = str_to_title(community)
  ) %>%
  select(geo_type, geo_name, geo_num = area_number, geometry, geometry_3435)


temp <- st_difference(muni, st_union(city))
hug <- bind_rows(temp, city)

mapview(list(city, muni, hug))

muni100 <- st_buffer(muni, 50, singleSide = TRUE)

hug <- st_join(muni, towns)
