library(dplyr)
library(here)
library(purrr)
library(sf)

school_path <- here("s3-bucket", "stable", "spatial", "school")

# UNIT
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "594e-g5w3?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_unified", "2016.geojson"),
    delete_dsn = TRUE
  )

# ELEMENTARY
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "an6r-bw5a?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_elementary", "2016.geojson"),
    delete_dsn = TRUE
  )