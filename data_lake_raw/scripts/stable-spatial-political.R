library(dplyr)
library(here)
library(purrr)
library(sf)

pol_path <- here("s3-bucket", "stable", "spatial", "political")

# BOARD OF REVIEW
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "7iyt-i4z4?method=export&format=GeoJSON"
)) %>%
  st_write(here(pol_path, "board_of_review", "2012.geojson"), delete_dsn = TRUE)

# COMMISSIONER DISTRICT
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "ihae-id2m?method=export&format=GeoJSON"
)) %>%
  st_write(here(pol_path, "commissioner", "2012.geojson"), delete_dsn = TRUE)

# JUDICIAL SUBCIRCUIT DISTRICT
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "54r3-ikn3?method=export&format=GeoJSON"
)) %>%
  st_write(here(pol_path, "judicial", "2012.geojson"), delete_dsn = TRUE)

# MUNICIPALITY
st_read(paste0(
  "https://datacatalog.cookcountyil.gov/api/geospatial/",
  "ta8t-zebk?method=export&format=GeoJSON"
)) %>%
  st_write(here(pol_path, "municipality", "2014.geojson"), delete_dsn = TRUE)

# WARD
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "xt4z-bnwh?method=export&format=GeoJSON"
)) %>%
  st_write(here(pol_path, "ward", "2003.geojson"), delete_dsn = TRUE)

st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "sp34-6z76?method=export&format=GeoJSON"
)) %>%
  st_write(here(pol_path, "ward", "2015.geojson"), delete_dsn = TRUE)
