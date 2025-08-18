library("ccao")
library("dplyr")
library("glue")
library("geoarrow")
library("mapview")
library("sf")
library("stringr")

county <- read_geoparquet_sf(
  "s3://ccao-data-warehouse-us-east-1/spatial/ccao/county/2019.parquet"
) %>%
  st_transform(3435) %>%
  select(geometry)

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
    geo_name = community
  ) %>%
  select(geo_type, geo_name, geo_num = area_number, geometry) %>%
  # Limit to our county boundary
  st_intersection(county)

# Ingest county municipalities
munis <- st_read(paste0(
  "https://gis.cookcountyil.gov/traditional/rest/services/",
  "politicalBoundary/MapServer/2/query?outFields=*&where=1%3D1&f=geojson"
)) %>%
  st_transform(3435) %>%
  mutate(
    geo_type = "municipality",
    geo_name = case_when(
      str_detect(AGENCY_DESC, "TWP") ~
        glue("UNINCORPORATED {str_remove(AGENCY_DESC, ' TWP')}"),
      TRUE ~ AGENCY_DESC
    ),
    geo_num = as.character(AGENCY)
  ) %>%
  select(
    geo_type,
    geo_name,
    geo_num,
    geometry
  ) %>%
  # Remove Chicago since we're using community areas
  filter(geo_name != "CITY OF CHICAGO")

# Adjust City of Chicago boundary to avoid gaps
buffered_city <- city %>%
  mutate(geometry = case_when(
    # Unfortunately the discrepancies between the city and county boundaries of
    # O'Hare are pretty large. We can be less aggressive with our buffer for the
    # rest of Chicago
    geo_name == "OHARE" ~ st_buffer(geometry, 1800),
    TRUE ~ st_buffer(geometry, 300)
  )) %>%
  # Move O'Hare to the bottom so it' gets cut the most's last during sequential
  # st_difference
  slice(
    which(geo_name != "OHARE"),
    which(geo_name == "OHARE")
  ) %>%
  # After we buffer, cut away any part that would overlap municipalities, be
  # outside the county, and parts of the city we already have
  st_difference(st_union(munis)) %>%
  st_intersection(county) %>%
  st_difference(st_union(city)) %>%
  # Add back a clean version of the city and merge community areas to their
  # buffers
  bind_rows(city) %>%
  group_by(geo_type, geo_name, geo_num) %>%
  summarise() %>%
  ungroup() %>%
  st_difference(st_union(munis)) %>%
  st_buffer(-1) %>%
  st_buffer(1) %>%
  # Sequential buffer to remove overlaps within buffered sections of community
  # areas
  st_difference()

output <- munis %>%
  bind_rows(buffered_city) %>%
  st_transform(4326) %>%
  mutate(geometry_3435 = st_transform(geometry, 3435))

st_write(output, "municipalities_community_areas.geojson", delete_dsn = TRUE)
