library(dplyr)
library(here)
library(purrr)
library(sf)

tax_path <- here("s3-bucket", "stable", "spatial", "tax")

# SSA
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "kjav-iyuj?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(tax_path, "ssa", "2020.geojson"),
    delete_dsn = TRUE
  )

# TIF
st_read(paste0(
  "https://data.cityofchicago.org/api/geospatial/",
  "fz5x-7zak?method=export&format=GeoJSON"
)) %>%
  st_write(
    here(tax_path, "tif", "2020.geojson"),
    delete_dsn = TRUE
  )