library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(zip)
library(tigris)
source("utils.R")

options(timeout = 200)

# This script retrieves environmental spatial data such as floodplain boundaries
# and proximity to train tracks/major roads
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")
current_year <- strftime(Sys.Date(), "%Y")

##### FEMA FLOODPLAINS #####
remote_file_flood_fema <- file.path(
  output_bucket, "flood_fema",
  paste0(current_year, ".geojson")
)


fema_url <- "https://hazards.fema.gov/femaportal/NFHL/Download/ProductsDownLoadServlet?DFIRMID=17031C&state=ILLINOIS&county=COOK%20COUNTY&fileName=17031C_" # nolint

fema_files <- c(
  "2021" = "20210615",
  "2022" = "20221130",
  "2023" = "20231006",
  "2025" = "20251218"
)


# Write FEMA floodplains to S3 if they don't exist
if (!aws.s3::object_exists(remote_file_flood_fema)) {
  # Found here: https://www.floodmaps.fema.gov/NFHL/status.shtml
  tmp_file <- tempfile(fileext = ".zip")
  tmp_dir <- tempdir()
  download.file(
    paste0(fema_url, fema_files[current_year], ".zip"),
    destfile = tmp_file,
    mode = "wb",
    timeout = 120
  )
  unzip(tmp_file, exdir = tmp_dir)

  tmp_file_flood_fema <- tempfile(fileext = ".geojson")
  st_read(file.path(tmp_dir, "S_FLD_HAZ_AR.shp")) %>%
    st_write(tmp_file_flood_fema)
  save_local_to_s3(remote_file_flood_fema, tmp_file_flood_fema)
  file.remove(tmp_file_flood_fema, tmp_file)
}


##### LAKE MICHICAN COASTLINE #####
walk(2013:current_year, function(i) {
  remote_file_coastline <- file.path(
    output_bucket, "coastline",
    paste0(i, ".geojson")
  )

  if (!aws.s3::object_exists(remote_file_coastline)) {
    tmp_file <- tempfile(fileext = ".geojson")

    st_write(
      tigris::coastline(year = i) %>%
        filter(NAME == "Great Lakes"),
      tmp_file
    )
    save_local_to_s3(remote_file_coastline, tmp_file)
    file.remove(tmp_file)
  }
})


##### COOK COUNTY HYDROLOGY #####
walk(2011:current_year, function(i) {
  remote_file_hydrology_area <- file.path(
    output_bucket, "hydrology", "area",
    paste0(i, ".geojson")
  )

  if (!aws.s3::object_exists(remote_file_hydrology_area)) {
    tmp_file <- tempfile(fileext = ".geojson")

    st_write(
      tigris::area_water("IL", "Cook", year = i),
      tmp_file
    )

    save_local_to_s3(remote_file_hydrology_area, tmp_file)
    file.remove(tmp_file)
  }

  remote_file_hydrology_linear <- file.path(
    output_bucket, "hydrology", "linear",
    paste0(i, ".geojson")
  )

  if (!aws.s3::object_exists(remote_file_hydrology_linear)) {
    tmp_file <- tempfile(fileext = ".geojson")

    st_write(
      tigris::linear_water("IL", "Cook", year = i),
      tmp_file
    )

    save_local_to_s3(remote_file_hydrology_linear, tmp_file)
    file.remove(tmp_file)
  }
})


##### RAILROAD #####
remote_file_railroad <- file.path(
  output_bucket, "railroad",
  paste0(2021, ".geojson")
)

# Write railroads to S3 if they don't exist
if (!aws.s3::object_exists(remote_file_railroad)) {
  tmp_file <- tempfile(fileext = ".geojson")
  tmp_dir <- tempdir()
  download.file(
    paste0(
      "https://opendata.arcgis.com/api/v3/",
      "datasets/dfa393be3a104c33a2ae95455c916ccd_3/downloads/",
      "data?format=geojson&spatialRefId=4326&where=1%3D1"
    ),
    destfile = tmp_file,
    mode = "wb"
  )

  save_local_to_s3(remote_file_railroad, tmp_file)
  file.remove(tmp_file)
}
