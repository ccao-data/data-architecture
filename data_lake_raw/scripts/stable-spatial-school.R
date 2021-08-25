library(dplyr)
library(here)
library(purrr)
library(sf)

school_path <- here("s3-bucket", "stable", "spatial", "school")

# DISTRICTS - UNIT
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "angb-d97z?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_unified", "2015.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "594e-g5w3?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_unified", "2016.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://opendata.arcgis.com/datasets/",
  "1e2f499e494744afb4ebae3a61d6e123_16.geojson"
)) %>%
  st_write(
    here(school_path, "school_district_unified", "2018.geojson"),
    delete_dsn = TRUE
  )

# DISTRICTS - ELEMENTARY
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "y8sv-9wex?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_elementary", "2015.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "an6r-bw5a?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_elementary", "2016.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://opendata.arcgis.com/datasets/",
  "cbcf6b1c3aaa420d90ccea6af877562b_2.geojson"
)) %>%
  st_write(
    here(school_path, "school_district_elementary", "2018.geojson"),
    delete_dsn = TRUE
  )

# DISTRICTS - SECONDARY
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "dagh-zphu?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_secondary", "2015.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "h3xu-azvs?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "school_district_secondary", "2016.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://opendata.arcgis.com/datasets/",
  "0657c2831de84e209863eac6c9296081_6.geojson"
)) %>%
  st_write(
    here(school_path, "school_district_secondary", "2018.geojson"),
    delete_dsn = TRUE
  )

# CPS ATTENDANCE - ELEMENTARY
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "e75y-e6uw?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_elementary", "2014-2015.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "asty-4xrr?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_elementary", "2015-2016.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "acyp-2sus?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_elementary", "2016-2017.geojson"),
    delete_dsn = TRUE
  )

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
  "47bj-3f4s?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_secondary", "2014-2015.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "vff3-x5qg?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_secondary", "2015-2016.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "bwum-4mhg?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_secondary", "2016-2017.geojson"),
    delete_dsn = TRUE
  )

st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "y9da-bb2y?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(school_path, "cps_attendance_secondary", "2017-2018.geojson"),
    delete_dsn = TRUE
  )