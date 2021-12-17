library(aws.s3)
library(dplyr)
library(sf)
library(zip)

# This script retrieves environmental spatial data such as floodplain boundaries
# and proximity to train tracks/major roads
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")


##### FEMA FLOODPLAINS #####
remote_file_flood_fema <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "flood_fema",
  paste0(current_year, ".geojson")
)

# Write FEMA floodplains to S3 if they don't exist
if (!aws.s3::object_exists(remote_file_flood_fema)) {
  # Found here: https://www.floodmaps.fema.gov/NFHL/status.shtml
  tmp_file <- tempfile(fileext = ".zip")
  tmp_dir <- tempdir()
  download.file(
    "https://hazards.fema.gov/femaportal/NFHL/Download/ProductsDownLoadServlet?DFIRMID=17031C&state=ILLINOIS&county=COOK%20COUNTY&fileName=17031C_20210615.zip",
    destfile = tmp_file, mode = "wb"
  )
  unzip(tmp_file, exdir = tmp_dir)

  tmp_file_flood_fema <- tempfile(fileext = ".geojson")
  st_read(file.path(tmp_dir, "S_FLD_HAZ_AR.shp")) %>%
    st_write(tmp_file_flood_fema)
  aws.s3::put_object(tmp_file_flood_fema, remote_file_flood_fema)
  file.remove(tmp_file_flood_fema, tmp_file)
}

##### LAKE MICHICAN COASTLINE #####
remote_file_coastline <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "coastline",
  paste0(current_year, ".geojson")
)

if (!aws.s3::object_exists(remote_file_coastline)) {
  tmp_file <- tempfile(fileext = ".geojson")

  st_write(
    tigris::coastline(year = current_year) %>% filter(NAME == "Great Lakes"),
    tmp_file
  )

  aws.s3::put_object(tmp_file, remote_file_coastline)
  file.remove(tmp_file)
}

##### COOK COUNTY HYDROLOGY #####
remote_file_hydrology <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "hydrology",
  paste0(current_year, ".geojson")
)

if (!aws.s3::object_exists(remote_file_hydrology)) {
  tmp_file <- tempfile(fileext = ".geojson")

  st_write(
    tigris::area_water("IL", "Cook", year = current_year),
    tmp_file
  )

  aws.s3::put_object(tmp_file, remote_file_hydrology)
  file.remove(tmp_file)
}
