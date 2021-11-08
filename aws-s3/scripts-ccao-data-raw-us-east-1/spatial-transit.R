library(aws.s3)
library(dplyr)
library(sf)
library(stringr)
library(zip)

# This script retrieves GTFS feeds for all Cook County public transit systems
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
bkt_path <- file.path(AWS_S3_RAW_BUCKET, "spatial", "transit")


##### CTA #####
# List of CTA feeds from Transit Feeds API
ccta_feed_dates_list <- c(
  "2015-10-29", "2016-09-30", "2017-10-22", "2018-10-06",
  "2019-10-04", "2020-10-10", "2021-10-09"
)

# If missing feed on S3, download and remove .htm file (causes errors)
# then rezip and upload
get_cta_feed <- function(feed_date) {
  feed_url <- paste0(
    "https://transitfeeds.com/p/chicago-transit-authority/165/",
    str_remove_all(feed_date, "-"), "/download"
  )
  s3_uri <- file.path(bkt_path, "cta", paste0(feed_date, "-gtfs.zip"))

  if (!aws.s3::object_exists(s3_uri)) {
    tmp_file <- tempfile(fileext = ".zip")
    tmp_dir <- tempdir()

    # Grab file from CTA, recompress without .htm file
    download.file(feed_url, destfile = tmp_file, mode = "wb")
    unzip(tmp_file, exdir = tmp_dir)
    zip::zipr(
      zipfile = tmp_file,
      files = list.files(tmp_dir, full.names = TRUE, pattern = ".txt")
    )
    aws.s3::put_object(tmp_file, s3_uri)
    file.remove(tmp_file)
  }
}

lapply(cta_feed_dates_list, get_cta_feed)


##### Metra #####
metra_feed_dates_list <- c(
  "2015-10-30", "2016-09-30", "2017-10-21", "2018-10-05",
  "2019-10-04", "2020-10-10", "2021-10-08"
)

get_metra_feed <- function(feed_date) {
  feed_url <- paste0(
    "https://transitfeeds.com/p/metra/169/",
    str_remove_all(feed_date, "-"), "/download"
  )
  s3_uri <- file.path(bkt_path, "metra", paste0(feed_date, "-gtfs.zip"))

  if (!aws.s3::object_exists(s3_uri)) {
    tmp_file <- tempfile(fileext = ".zip")
    download.file(feed_url, destfile = tmp_file, mode = "wb")
    aws.s3::put_object(tmp_file, s3_uri)
    file.remove(tmp_file)
  }
}

lapply(metra_feed_dates_list, get_metra_feed)


##### Pace #####
pace_feed_dates_list <- c(
  "2015-10-16", "2016-10-15", "2017-10-16", "2018-10-17",
  "2019-10-22", "2020-09-23", "2021-03-15"
)

get_pace_feed <- function(feed_date) {
  feed_url <- paste0(
    "https://transitfeeds.com/p/pace/171/",
    str_remove_all(feed_date, "-"), "/download"
  )
  s3_uri <- file.path(bkt_path, "pace", paste0(feed_date, "-gtfs.zip"))

  if (!aws.s3::object_exists(s3_uri)) {
    tmp_file <- tempfile(fileext = ".zip")
    download.file(feed_url, destfile = tmp_file, mode = "wb")
    aws.s3::put_object(tmp_file, s3_uri)
    file.remove(tmp_file)
  }
}

lapply(pace_feed_dates_list, get_pace_feed)

# Cleanup
rm(list = ls())