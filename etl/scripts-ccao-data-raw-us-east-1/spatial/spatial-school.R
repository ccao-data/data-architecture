library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves school district and attendance boundaries published by
# the various districts around Cook County
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "school")

sources_list <- bind_rows(list(
  # CPS ATTENDANCE - ELEMENTARY
  "attendance_ele_0607" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "qbay-3nnc?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2006-2007"
  ),
  "attendance_ele_0708" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "r2h7-fxir?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2007-2008"
  ),
  "attendance_ele_0809" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "8jx5-pt46?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2008-2009"
  ),
  "attendance_ele_0910" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "sra3-5rba?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2009-2010"
  ),
  "attendance_ele_1011" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "7jn2-4muy?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2010-2011"
  ),
  "attendance_ele_1112" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "6tkx-ju8g?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2011-2012"
  ),
  "attendance_ele_1213" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "rfrd-v47v?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2012-2013"
  ),
  "attendance_ele_1314" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "g7sv-g285?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2013-2014"
  ),
  "attendance_ele_1415" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "e75y-e6uw?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2014-2015"
  ),
  "attendance_ele_1516" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "asty-4xrr?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2015-2016"
  ),
  "attendance_ele_1617" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "acyp-2sus?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2016-2017"
  ),
  "attendance_ele_1718" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "u959-tya7?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2017-2018"
  ),
  "attendance_ele_1819" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "whkz-sk6f?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2018-2019"
  ),
  "attendance_ele_1920" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "abk6-gwwr?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2019-2020"
  ),
  "attendance_ele_2021" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "gaak-qc7r?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2020-2021"
  ),
  "attendance_ele_2022" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "a3xm-ett9?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2021-2022"
  ),
  "attendance_ele_2023" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "d8hd-y5ce?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2022-2023"
  ),
  "attendance_ele_2024" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "8k6e-w34s?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2023-2024"
  ),
  "attendance_ele_2025" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "td7k-bjgv?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_elementary",
    "year" = "2024-2025"
  ),

  # CPS ATTENDANCE - SECONDARY
  "attendance_sec_0607" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "rwav-ux96?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2006-2007"
  ),
  "attendance_sec_0708" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "wifq-a78y?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2007-2008"
  ),
  "attendance_sec_0809" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "fede-88y6?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2008-2009"
  ),
  "attendance_sec_0910" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "csqr-8bm8?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2009-2010"
  ),
  "attendance_sec_1011" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "4ytu-3pkn?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2010-2011"
  ),
  "attendance_sec_1112" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "s7sc-q48d?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2011-2012"
  ),
  "attendance_sec_1213" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "9xc2-zddf?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2012-2013"
  ),
  "attendance_sec_1314" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "azwk-fxgp?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2013-2014"
  ),
  "attendance_sec_1415" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "47bj-3f4s?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2014-2015"
  ),
  "attendance_sec_1516" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "vff3-x5qg?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2015-2016"
  ),
  "attendance_sec_1617" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "bwum-4mhg?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2016-2017"
  ),
  "attendance_sec_1718" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "y9da-bb2y?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2017-2018"
  ),
  "attendance_sec_1819" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "bv6n-449d?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2018-2019"
  ),
  "attendance_sec_1920" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "d95y-ue9h?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2019-2020"
  ),
  "attendance_sec_2021" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "da2c-wnfg?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2020-2021"
  ),
  "attendance_sec_2022" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "is3f-j4ke?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2021-2022"
  ),
  "attendance_sec_2023" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "4m25-hh4h?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2022-2023"
  ),
  "attendance_sec_2024" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "gba7-ip5a?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2023-2024"
  ),
  "attendance_sec_2025" = c(
    "source" = "https://data.cityofchicago.org/api/geospatial/",
    "api_url" = "cczt-jtaj?method=export&format=GeoJSON",
    "boundary" = "cps_attendance_secondary",
    "year" = "2024-2025"
  ),

  # LOCATION
  "locations_all_21" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "a9a2e342397249fd90872765d11aede7_4.geojson",
    "boundary" = "location",
    "year" = "2021"
  )
))

# Function to call referenced API, pull requested data, and write it to S3
pwalk(sources_list, function(...) {
  df <- tibble::tibble(...)
  open_data_to_s3(
    s3_bucket_uri = output_bucket,
    base_url = df$source,
    data_url = df$api_url,
    dir_name = df$boundary,
    file_year = df$year,
    file_ext = ".geojson"
  )
})


##### County-provided District Files #####

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//10.122.19.14/ArchiveServices"

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
      "school_district_elementary",
      "school_district_secondary",
      "school_district_unified"
    ),
    layer = c(
      "ElemSchlTaxDist",
      "HighSchlTaxDist",
      "UnitSchlTaxDist"
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
