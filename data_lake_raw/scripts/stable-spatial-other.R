library(dplyr)
library(here)
library(purrr)
library(sf)

oth_path <- here("s3-bucket", "stable", "spatial", "other")
cur_year <- format(Sys.Date(), "%Y")

# CHICAGO COMMUNITY AREA
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "cauq-8yn6?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(oth_path, "community_area", paste0(cur_year, ".geojson")),
    delete_dsn = TRUE
  )

# compress geojson using gzip
gzip(filename = here(oth_path, "community_area", paste0(cur_year, ".geojson")),
     destname = here(oth_path, "community_area", paste0(cur_year, ".geojson.gz")))

# UNINCORPORATED AREA
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "kbr6-dyec?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(oth_path, "unincorporated_area", "2014.geojson"),
    delete_dsn = TRUE
  )

# compress geojson using gzip
gzip(filename = here(oth_path, "unincorporated_area", "2014.geojson"),
     destname = here(oth_path, "unincorporated_area", "2014.geojson.gz"))