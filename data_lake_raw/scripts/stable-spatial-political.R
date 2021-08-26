library(dplyr)
library(here)
library(purrr)
library(sf)

pol_path <- here("s3-bucket", "stable", "spatial", "political")

api_info <- list(
  # BOARD OF REVIEW
  "bor_2012" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "7iyt-i4z4?method=export&format=GeoJSON",
                 "boundary" = "board_of_review",
                 "year"     = "2012"),

  # COMMISSIONER DISTRICT
  "cmd_2012" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "ihae-id2m?method=export&format=GeoJSON",
                 "boundary" = "commissioner",
                 "year"     = "2012"),

  # CONGRESSIONAL DISTRICT
  "cnd_2010" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "jh56-md8x?method=export&format=GeoJSON",
                 "boundary" = "congressionial_district",
                 "year"     = "2010"),

  # JUDICIAL SUBCIRCUIT DISTRICT
  "jsd_2012" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "54r3-ikn3?method=export&format=GeoJSON",
                 "boundary" = "judicial",
                 "year"     = "2012"),

  # MUNICIPALITY
  "mnc_2014" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "ta8t-zebk?method=export&format=GeoJSON",
                 "boundary" = "municipality",
                 "year"     = "2014"),

  # STATE REPRESENTATIVE
  "str_2010" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "gsew-ir9y?method=export&format=GeoJSON",
                 "boundary" = "state_representative",
                 "year"     = "2010"),

  # STATE SENATE
  "sts_2010" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "ezne-sr8y?method=export&format=GeoJSON",
                 "boundary" = "state_senate",
                 "year"     = "2010"),

  # WARD
  "wrd_2003" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                 "api_url"  = "xt4z-bnwh?method=export&format=GeoJSON",
                 "boundary" = "ward",
                 "year"     = "2003"),
  "wrd_2015" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                 "api_url"  = "sp34-6z76?method=export&format=GeoJSON",
                 "boundary" = "ward",
                 "year"     = "2015")

)

# function to call referenced API, pull requested data, and write it to specified file path
pull_and_write <- function(x) {

  current_file <- here(pol_path, x["boundary"], paste0(x["year"], ".geojson"))

  if (!file.exists(current_file)) {

    st_read(paste0(x["source"], x["api_url"])) %>%

      st_write(current_file, delete_dsn = TRUE)

  }

}

# apply function to "api_info"
lapply(api_info, pull_and_write)

# clean
rm(list = ls())