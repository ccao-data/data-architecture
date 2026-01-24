library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
library(zip)
source("utils.R")

# This script retrieves GTFS feeds for all Cook County
# public transit systems. NOTE: Certain early feeds are not enumerated
# by date, and so must be added to S3 manually
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_path <- file.path(AWS_S3_RAW_BUCKET, "spatial", "transit")

# Make sure download.file doesn't timeout for larger files
options(timeout = max(300, getOption("timeout")))

##### CTA #####
# List of CTA feeds from Transit Feeds API
cta_feed_dates_list <- c(
  "2015-10-29", "2016-09-30", "2017-10-22", "2018-10-06",
  "2019-10-04", "2020-10-10", "2021-10-09", "2022-10-20",
  "2023-10-04", "2024-10-17-0023", "2025-12-19-0112"
)

# If missing feed on S3, download and remove .htm file (causes errors)
# then rezip and upload
get_cta_feed <- function(feed_date) {
  feed_url <- ifelse(
    substr(feed_date, 1, 4) <= "2023",
    paste0(
      "https://transitfeeds.com/p/chicago-transit-authority/165/",
      str_remove_all(feed_date, "-"), "/download"
    ),
    paste0(
      "https://files.mobilitydatabase.org/mdb-389/mdb-389-",
      str_remove_all(feed_date, "-"),
      "/mdb-389-",
      str_remove_all(feed_date, "-"),
      ".zip"
    )
  )
  s3_uri <- file.path(output_path, "cta", paste0(feed_date, "-gtfs.zip"))

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
    save_local_to_s3(s3_uri, tmp_file)
    file.remove(tmp_file)
  }
}

walk(cta_feed_dates_list, get_cta_feed)


##### Metra #####
metra_feed_dates_list <- c(
  "2015-10-30", "2016-09-30", "2017-10-21", "2018-10-05",
  "2019-10-04", "2020-10-10", "2021-10-08", "2022-10-21",
  "2023-10-14", "2024-04-22", "2025-12-31"
)

get_metra_feed <- function(feed_date) {
  feed_url <- if (substr(feed_date, 1, 4) <= "2023") {
    paste0(
      "https://transitfeeds.com/p/metra/169/",
      str_remove_all(feed_date, "-"), "/download"
    )
  } else if (substr(feed_date, 1, 4) == "2024") {
    paste0(
      "https://files.mobilitydatabase.org/mdb-1187/mdb-1187-",
      str_remove_all(feed_date, "-"),
      "0016/mdb-1187-",
      str_remove_all(feed_date, "-"),
      "0016.zip"
    )
  } else {
    # Unfortunately the feed number for metra changed after 2024
    paste0(
      "https://files.mobilitydatabase.org/mdb-2854/mdb-2854-",
      str_remove_all(feed_date, "-"),
      "0051/mdb-2854-",
      str_remove_all(feed_date, "-"),
      "0051.zip"
    )
  }


  s3_uri <- file.path(output_path, "metra", paste0(feed_date, "-gtfs.zip"))

  if (!aws.s3::object_exists(s3_uri)) {
    tmp_file <- tempfile(fileext = ".zip")
    download.file(feed_url, destfile = tmp_file, mode = "wb")

    save_local_to_s3(s3_uri, tmp_file)
    file.remove(tmp_file)
  }
}

walk(metra_feed_dates_list, get_metra_feed)


##### Pace #####
pace_feed_dates_list <- c(
  "2015-10-16", "2016-10-15", "2017-10-16", "2018-10-17",
  "2019-10-22", "2020-09-23", "2021-03-15", "2023-09-24",
  "2024-02-07", "2025-12"
)

# Find most recent feed here, under "Feed Location":
# https://www.pacebus.com/route-timetable-data-services

get_pace_feed <- function(feed_date) {
  feed_url <- if (feed_date < "2023-09-24") {
    paste0(
      "https://transitfeeds.com/p/pace/171/",
      str_remove_all(feed_date, "-"), "/download"
    )
  } else {
    paste0(
      "https://www.pacebus.com/sites/default/files/",
      substr(feed_date, 1, 7),
      "/GTFS.zip"
    )
  }
  s3_uri <- file.path(output_path, "pace", paste0(feed_date, "-gtfs.zip"))

  if (!aws.s3::object_exists(s3_uri)) {
    tmp_file <- tempfile(fileext = ".zip")
    download.file(feed_url, destfile = tmp_file, mode = "wb")

    save_local_to_s3(s3_uri, tmp_file)
    file.remove(tmp_file)
  }
}

walk(pace_feed_dates_list, get_pace_feed)
