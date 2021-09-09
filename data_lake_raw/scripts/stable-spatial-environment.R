library(dplyr)
library(here)
library(purrr)
library(sf)
library(zip)
library(tidytransit)

environment_path <- here("s3-bucket", "stable", "spatial", "environment")

# CTA LINES
tmp_file <- tempfile(fileext = ".zip")
tmp_dir <- tempdir()

# grab files from openmobility data, recompress without .htm file
download.file(
  "http://www.transitchicago.com/downloads/sch_data/google_transit.zip",
  destfile = tmp_file, mode = "wb"
)

unzip(tmp_file, exdir = tmp_dir)

zip::zipr(zipfile = tmp_file, files = list.files(tmp_dir, full.names = TRUE, pattern = ".txt"))

# convert data to simple feature
read_gtfs(tmp_file) %>%
  gtfs_as_sf() %>%

  # filter out bus stops
  get_route_geometry(route_ids = c("Red", "P", "Y", "Blue", "Pink", "G", "Org" ,"Brn")) %>%

  # write data as geojson
  st_write(file.path(environment_path, "cta_line", "2020.geojson"), delete_dsn = TRUE)

# FEMA FLOODPLAINS
# found here: https://www.floodmaps.fema.gov/NFHL/status.shtml
tmp_file <- tempfile()
download.file(
  "https://hazards.fema.gov/femaportal/NFHL/Download/ProductsDownLoadServlet?DFIRMID=17031C&state=ILLINOIS&county=COOK%20COUNTY&fileName=17031C_20210615.zip",
  destfile = tmp_file, mode = "wb"
)

unzip(tmp_file, exdir = tmp_dir)

st_read(file.path(tmp_dir, "S_FLD_HAZ_AR.shp")) %>%

  # write data as geojson
  st_write(file.path(environment_path, "flood_fema", "2021.geojson"))
