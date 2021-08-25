library(dplyr)
library(here)
library(purrr)
library(sf)

school_path <- here("s3-bucket", "stable", "spatial", "school")

# DISTRICTS - UNIT
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "594e-g5w3?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_unified", "2016.geojson"),
    delete_dsn = TRUE
  )

# DISTRICTS - ELEMENTARY
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "an6r-bw5a?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_elementary", "2016.geojson"),
    delete_dsn = TRUE
  )

# CPS ATTENDANCE - ELEMENTARY
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "u959-tya7?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_elementary", "2017-2018.geojson"),
    delete_dsn = TRUE
  )

# CPS ATTENDANCE - SECONDARY
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "y9da-bb2y?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_secondary", "2017-2018.geojson"),
    delete_dsn = TRUE
  )
