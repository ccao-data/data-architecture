library(dplyr)
library(here)
library(purrr)
library(sf)
library(zip)
library(tidytransit)

access_path <- here("s3-bucket", "stable", "spatial", "access")

api_info <- list(
  # INDUSTRIAL CORRIDORS
  "ind_2013" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                 "api_url"  = "e6xh-nr8w?method=export&format=GeoJSON",
                 "boundary" = "industrial_corridor",
                 "year"     = "2013")
)


# function to call referenced API, pull requested data, and write it to specified file path
pull_and_write <- function(x) {

  current_file <- here(access_path, x["boundary"], paste0(x["year"], ".geojson"))

  if (!file.exists(current_file)) {

    st_read(paste0(x["source"], x["api_url"])) %>%

      st_write(current_file, delete_dsn = TRUE)

  }

}

# apply function to "api_info"
lapply(api_info, pull_and_write)

# CTA STOPS
tmp_file <- tempfile(fileext = ".zip")
tmp_dir <- tempdir()

# grab files from openmobility data, recompress without .htm file
download.file(
  "http://www.transitchicago.com/downloads/sch_data/google_transit.zip",
  destfile = tmp_file, mode = "wb"
)

unzip(tmp_file, exdir = tmp_dir)

zip::zipr(zipfile = tmp_file, files = list.files(tmp_dir, full.names = TRUE, pattern = ".txt"))

# filter out bus stops
read_gtfs(tmp_file) %>%
filter_stops(route_ids = c("Red", "P", "Y", "Blue", "Pink", "G", "Org" ,"Brn"),
               service_ids = gtfs$calendar %>% pull(service_id)) %>%

  # make sure data is unique by stop
  distinct(stop_name, .keep_all = TRUE) %>%
  st_as_sf(coords = c("stop_lon", "stop_lat")) %>%

  # write data as geojson
  st_write(file.path(access_path, "cta_stop", "2020.geojson"), delete_dsn = TRUE)

# clean
rm(list = ls())