library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(tidyr)
source("utils.R")

# This script retrieves the boundaries of various Cook County taxing districts
# and entities, such as TIFs, libraries, etc.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "tax")

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//gisemcv1.ccounty.com/ArchiveServices/"

# Tax districts
crossing(

  # Paths for all relevant geodatabases
  data.frame("path" = list.files(file_path, full.names = TRUE)) %>%
    filter(
      str_detect(path, "Current", negate = TRUE) &
        str_detect(path, "20") &
        str_detect(path, "Admin")
    ),

  # S3 paths and corresponding layers
  data.frame(
    dir_name = c(
      "community_college_district",
      "fire_protection_district",
      "library_district",
      "park_district",
      "sanitation_district",
      "special_service_area",
      "tif_district"
      ),
    layer = c(
      "CommCollTaxDist",
      "FireProTaxDist",
      "LibrTaxDist",
      "ParkTaxDist",
      "SanitaryTaxDist",
      "SpecServTaxDist",
      "TIFTaxDist"
      )
  )

) %>%
  arrange(dir_name, path) %>%

  # Function to call referenced GDBs, pull requested data, and write it to S3
  pwalk(function(...) {
    df <- tibble::tibble(...)
    county_gdb_to_s3(
      s3_bucket_uri = output_bucket,
      dir_name = df$dir_name,
      file_path = df$path,
      layer = df$layer
    )
  })
