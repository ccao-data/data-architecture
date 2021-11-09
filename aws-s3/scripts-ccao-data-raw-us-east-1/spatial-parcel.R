library(aws.s3)
library(dplyr)
library(sf)

# This script retrieves the parcel files from Cook Central and saves them as
# geojson on S3
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

api_info <- list(
  c("api_url" = "983b136927b5418986e86ba8b131991f_0.geojson", "year" = "2000"),
  c("api_url" = "7bdf70f3ee6b48819f822d086f808669_0.geojson", "year" = "2001"),
  c("api_url" = "2d7f0639172b4506bd2e34558359089f_0.geojson", "year" = "2002"),
  c("api_url" = "91062410b21f48969b1dd78b1bb4e551_0.geojson", "year" = "2003"),
  c("api_url" = "7e05920c5ea742299ba7cb08a763f418_0.geojson", "year" = "2004"),
  c("api_url" = "ea01ea778e8e40e1a7af8d981e00aca4_0.geojson", "year" = "2005"),
  c("api_url" = "6a9b312ebc7f4747bef2933401462ca6_0.geojson", "year" = "2006"),
  c("api_url" = "2b1fe254468c416daa78dbe220f6388a_0.geojson", "year" = "2007"),
  c("api_url" = "f7bf69fea4b54017934d9f0b318254fa_0.geojson", "year" = "2008"),
  c("api_url" = "95d430756b9c45eea4f2c20dae32dbe6_0.geojson", "year" = "2009"),
  c("api_url" = "b9d9d454265842d1a9c9d49979afec52_0.geojson", "year" = "2010"),
  c("api_url" = "fde87c7b397745dfb42503d7c37ea9d5_0.geojson", "year" = "2011"),
  c("api_url" = "bd26024e1c6546d6a86ad384a7a31765_0.geojson", "year" = "2012"),
  c("api_url" = "ea846a11e7a64c6eb7eafcd132c88484_0.geojson", "year" = "2013"),
  c("api_url" = "f2d470e08ab441229f6d310fb8c625ab_0.geojson", "year" = "2014"),
  c("api_url" = "cb0a110357284b1ab23dedc6d0a34c57_0.geojson", "year" = "2015"),
  c("api_url" = "0b86fc37d99c413a8b70a1c2bfc895ba_0.geojson", "year" = "2016"),
  c("api_url" = "a45722101ed8491fb71930fd4c2c64ab_0.geojson", "year" = "2017"),
  c("api_url" = "9539568a52124b99addb042efd0f83b1_0.geojson", "year" = "2018"),
  c("api_url" = "3d3375ac11d147308815d5cf4bb43f4e_0.geojson", "year" = "2019"),
  c("api_url" = "577d80fcbf0441a780ecdfd9e1b6b5c2_0.geojson", "year" = "2020")
)


# Function to call referenced API, pull requested data, and write it to S3
pull_and_write <- function(x) {

  tmp_file <- tempfile(fileext = ".geojson")
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "parcel",
    paste0(x["year"], ".geojson")
  )

  if (!aws.s3::object_exists(remote_file)) {

    st_read(paste0("https://opendata.arcgis.com/datasets/", x["api_url"])) %>%
      st_write(tmp_file, delete_dsn = TRUE)

    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)
  }
}

# Apply function to "api_info"
lapply(api_info, pull_and_write)

# Cleanup
rm(list = ls())