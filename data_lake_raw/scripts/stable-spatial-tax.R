library(dplyr)
library(here)
library(purrr)
library(sf)

tax_path <- here("s3-bucket", "stable", "spatial", "tax")

api_info <- list(
  # TIF
  "tif_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "si4i-nrtg?method=export&format=GeoJSON",
                 "boundary" = "tif",
                 "year"     = "2015"),

  "tif_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "5dup-xpsj?method=export&format=GeoJSON",
                 "boundary" = "tif",
                 "year"     = "2016"),

  "tif_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "cc516b88a47547bd974bba4ebc120ecf_15.geojson",
                 "boundary" = "tif",
                 "year"     = "2018"),

  # SSA
  "tif_2018" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                 "api_url"  = "fz5x-7zak?method=export&format=GeoJSON",
                 "boundary" = "ssa",
                 "year"     = "2020")
)

# function to call referenced API, pull requested data, and write it to specified file path
pull_and_write <- function(x) {

  current_file <- here(tax_path, x["boundary"], paste0(x["year"], ".geojson"))

  if (!file.exists(current_file)) {

    st_read(paste0(x["source"], x["api_url"])) %>%

      st_write(current_file, delete_dsn = TRUE)

  }

}

# apply function to "api_info"
lapply(api_info, pull_and_write)

# clean
rm(list = ls())