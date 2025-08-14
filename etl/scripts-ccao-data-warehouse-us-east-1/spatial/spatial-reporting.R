library("ccao")
library("dplyr")
library("geoarrow")
library("mapview")
library("sf")
library("stringr")

county <- read_geoparquet_sf(
  "s3://ccao-data-warehouse-us-east-1/spatial/ccao/county/2019.parquet"
) %>%
  st_transform(3435) %>%
  select(geometry)

# Ingest CCAO townships
towns <- ccao::town_shp %>%
  st_transform(3435) %>%
  select(township_name)

# Ingest City of Chicago community areas
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
  select(geo_type, geo_name, geo_num = area_number, geometry)

# Ingest county municipalities
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
    geometry
  )

# Adjust City of Chicago boundary to avoid gaps
buffered_city <- city %>%
  st_buffer(2000) %>%
  st_intersection(
    munis %>%
      filter(geo_name == "City Of Chicago")
  ) %>%
  st_difference(st_union(city)) %>%
  select(geo_type, geo_name, geo_num, geometry) %>%
  bind_rows(city) %>%
  group_by(geo_type, geo_name, geo_num) %>%
  summarize()

unincorporated <- towns %>%
  st_intersection(
    munis %>%
      filter(geo_name == "Unincorporated")
  ) %>%
  mutate(geo_name = paste(geo_name, township_name)) %>%
  select(-township_name) %>%
  st_difference(st_union(buffered_city))

munis <- munis %>%
  filter(!(geo_name %in% c("Unincorporated", "City Of Chicago"))) %>%
  st_difference(st_union(buffered_city))

output <- munis %>%
  bind_rows(buffered_city) %>%
  bind_rows(unincorporated) %>%
  st_intersection(county)

mapview(list(orig, unincorporated, munis))
