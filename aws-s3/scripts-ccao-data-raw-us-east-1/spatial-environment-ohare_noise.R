library(aws.s3)
library(dplyr)
library(sf)

# This script retrieves and uploads a couple of PDFs containing
# Ohare noise level measurements and addresses of sensors, as well as
# a shapefile containing a theoretical noise boundary
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")


##### OHARE NOISE MONITORS #####
# File names
files <- c("ORD_Fact_Sheet_Monitors_History.pdf", "ORD_Fact_Sheet_Monitors_Introduction.pdf")


pull_and_write_ohare_noise <- function(x) {

  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "environment",
    "ohare_noise", "monitor", x
  )

  # Print file being written
  print(paste0(Sys.time(), " - ", remote_file))

  # Check to see if file already exists on S3; if it does, skip it
  if (!aws.s3::object_exists(remote_file)) {

    # grab files
    tmp_file <- tempfile(fileext = ".pdf")
    tmp_dir <- tempdir()

    download.file(
      paste0("https://www.oharenoise.org/sitemedia/documents/noise_mitigation/noise_monitors/", x),
      destfile = tmp_file,
      mode = "wb"
    )

    # Write to S3
    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)

  }

}

# Apply function
lapply(files, pull_and_write_ohare_noise)


##### OHARE NOISE CONTOUR #####
# This file isn't available online
ohare_noise_boundary <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "environment",
  "ohare_noise", "contour", "ORD_2016_Noise_Contour.geojson"
)

if (!aws.s3::object_exists(ohare_noise_boundary)) {

  # Grab file
  tmp_file <- tempfile(fileext = ".geojson")
  tmp_dir <- tempdir()
  st_write(
    st_read(
      paste0("//fileserver/ocommon/Communications Map",
             "/Airport Maps/Maine/O'Hare_Noise_Countour.shp"),
    ),
    tmp_file
  )

  # Write to S3
  aws.s3::put_object(tmp_file, ohare_noise_boundary)
  file.remove(tmp_file)
}
