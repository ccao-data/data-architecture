library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

################################################################################
# This is static data and this script does not need to be re-run unless the data
# is no longer availale in the Data Department's raw S3 bucket.
################################################################################

# This script retrieves and uploads a couple of PDFs containing
# O'Hare noise level measurements and addresses of sensors, as well as
# a shapefile containing a theoretical noise boundary
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")

##### OHARE NOISE MONITORS #####
# File names
files <- c(
  "ORD_Fact_Sheet_Monitors_History.pdf",
  "ORD_Fact_Sheet_Monitors_Introduction.pdf"
)

pull_and_write_ohare_noise <- function(x) {
  remote_file <- file.path(output_bucket, "ohare_noise_monitor", x)

  # Print file being written
  message(Sys.time(), " - ", remote_file)

  # Check to see if file already exists on S3; if it does, skip it
  if (!aws.s3::object_exists(remote_file)) {
    # Grab files
    tmp_file <- tempfile(fileext = ".pdf")
    tmp_dir <- tempdir()

    download.file(
      paste0(
        "https://www.oharenoise.org/sitemedia/documents/",
        "noise_mitigation/noise_monitors/",
        x
      ),
      destfile = tmp_file,
      mode = "wb"
    )

    # Write to S3
    save_local_to_s3(remote_file, tmp_file)
    file.remove(tmp_file)
  }
}

# Apply function
walk(files, pull_and_write_ohare_noise)


##### OHARE NOISE CONTOUR #####
# This file isn't available online
remote_file_ohare_noise_boundary <- file.path(
  output_bucket, "ohare_noise_contour",
  "ORD_2016_Noise_Contour.geojson"
)
tmp_file_ohare_noise_boundary <- paste0(
  "//fileserver/ocommon/Communications Map",
  "/Airport Maps/Maine/O'Hare_Noise_Countour.shp"
)
save_local_to_s3(
  remote_file_ohare_noise_boundary,
  tmp_file_ohare_noise_boundary
)
