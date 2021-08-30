library(dplyr)
library(here)
library(purrr)
library(sf)
library(readr)

access_path <- here("s3-bucket", "stable", "spatial", "access")

# CTA STOPS
tmp_file <- tempfile(fileext = ".zip")
tmp_dir <- tempdir()

# grab files from openmobility data
download.file(
  "http://www.transitchicago.com/downloads/sch_data/google_transit.zip",
  destfile = tmp_file, mode = "wb"
)

unzip(tmp_file, exdir = tmp_dir)

# read in gtfs file, convert to geojson and write it
read_delim(file.path(tmp_dir, "stops.txt"), delim = ",") %>%

st_as_sf(coords = c("stop_lon", "stop_lat"), crs = 4326) %>%

  st_write(here(access_path, "cta_stop", "2020.geojson"), delete_dsn = TRUE)
