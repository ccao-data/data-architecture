library(aws.s3)
library(dplyr)
library(sf)
library(zip)
library(tidytransit)

# This script retrieves environmental spatial data such as floodplain boundaries
# and proximity to train tracks/major roads
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")


# CTA LINES
remote_file_cta_feed <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "access", "cta_stop", "feed",
  paste0(current_year, ".zip")
)
remote_file_cta_line <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "cta_line",
  paste0(current_year, ".geojson")
)

# If missing feed on S3, download and remove .htm file (causes errors)
# then rezip and upload
if (!aws.s3::object_exists(remote_file_cta_feed)) {
  tmp_file <- tempfile(fileext = ".zip")
  tmp_dir <- tempdir()

  # Grab file from CTA, recompress without .htm file
  download.file(
    "http://www.transitchicago.com/downloads/sch_data/google_transit.zip",
    destfile = tmp_file, mode = "wb"
  )
  unzip(tmp_file, exdir = tmp_dir)
  zip::zipr(
    zipfile = tmp_file,
    files = list.files(tmp_dir, full.names = TRUE, pattern = ".txt")
  )
  aws.s3::put_object(tmp_file, remote_file_cta_feed)
  file.remove(tmp_file)
}


# Upload train line geometry if missing
if (!aws.s3::object_exists(remote_file_cta_line)) {

  # Grab saved feed from S3
  tmp_feed <- aws.s3::save_object(
    remote_file_cta_feed,
    file = tempfile(fileext = ".zip")
  )

  # Convert GTFS data to simple feature
  tmp_file_line <- tempfile(fileext = ".geojson")
  read_gtfs(tmp_feed) %>%
    gtfs_as_sf() %>%

    # Filter out bus stops
    get_route_geometry(
      route_ids = .$routes %>% filter(route_type == 1) %>% pull(route_id)
    ) %>%
    st_write(tmp_file_line)

  aws.s3::put_object(tmp_file_line, remote_file_cta_line)
  file.remove(tmp_file_line)
}


# FEMA FLOODPLAINS
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

# OHARE NOISE CONTOUR
# this file isn't avaiable online
ohare_noise <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment", "ohare_noise", "2016.geojson"
)

if (!aws.s3::object_exists(ohare_noise)) {

  # grab file
  tmp_file <- tempfile(fileext = ".geojson")
  tmp_dir <- tempdir()

  st_write(
    st_read(
      "//fileserver/ocommon/Communications Map/Airport Maps/Maine/O'Hare_Noise_Countour.shp"
      ),
    tmp_file
    )

  # Write to S3
  aws.s3::put_object(tmp_file, ohare_noise)
  file.remove(tmp_file)

}
