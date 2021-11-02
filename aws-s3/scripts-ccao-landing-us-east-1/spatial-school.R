library(aws.s3)
library(dplyr)
library(sf)

# This script retrieves school district and attendance boundaries published by
# the various districts around Cook County
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

api_info <- list(
  # DISTRICTS - UNIT
  "districts_unit_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "angb-d97z?method=export&format=GeoJSON",
                            "boundary" = "school_district_unified",
                            "year"     = "2015"),

  "districts_unit_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "594e-g5w3?method=export&format=GeoJSON",
                            "boundary" = "school_district_unified",
                            "year"     = "2016"),

  "districts_unit_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                            "api_url"  = "1e2f499e494744afb4ebae3a61d6e123_16.geojson",
                            "boundary" = "school_district_unified",
                            "year"     = "2018"),

  # DISTRICTS - ELEMENTARY
  "districts_elem_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "y8sv-9wex?method=export&format=GeoJSON",
                            "boundary" = "school_district_elementary",
                            "year"     = "2015"),

  "districts_elem_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "an6r-bw5a?method=export&format=GeoJSON",
                            "boundary" = "school_district_elementary",
                            "year"     = "2016"),

  "districts_elem_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                            "api_url"  = "cbcf6b1c3aaa420d90ccea6af877562b_2.geojson",
                            "boundary" = "school_district_elementary",
                            "year"     = "2018"),

  # DISTRICTS - SECONDARY
  "districts_scnd_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "dagh-zphu?method=export&format=GeoJSON",
                            "boundary" = "school_district_secondary",
                            "year"     = "2015"),

  "districts_scnd_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "h3xu-azvs?method=export&format=GeoJSON",
                            "boundary" = "school_district_secondary",
                            "year"     = "2016"),

  "districts_scnd_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                            "api_url"  = "0657c2831de84e209863eac6c9296081_6.geojson",
                            "boundary" = "school_district_secondary",
                            "year"     = "2018"),

  # CPS ATTENDANCE - ELEMENTARY
  "attendance_ele_1415" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "e75y-e6uw?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2014-2015"),

  "attendance_ele_1516" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "asty-4xrr?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2015-2016"),

  "attendance_ele_1617" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "acyp-2sus?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2016-2017"),

  "attendance_ele_1718" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "u959-tya7?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2017-2018"),

  # CPS ATTENDANCE - SECONDARY
  "attendance_sec_1415" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "47bj-3f4s?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2014-2015"),

  "attendance_sec_1516" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "vff3-x5qg?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2015-2016"),

  "attendance_sec_1617" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "bwum-4mhg?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2016-2017"),

  "attendance_sec_1718" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "y9da-bb2y?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2017-2018")
)

# Function to call referenced API, pull requested data, and write it to S3
pull_and_write <- function(x) {

  tmp_file <- tempfile(fileext = ".geojson")
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "school",
    x["boundary"], paste0(x["year"], ".geojson")
  )

  if (!aws.s3::object_exists(remote_file)) {

    st_read(paste0(x["source"], x["api_url"])) %>%
      st_write(tmp_file, delete_dsn = TRUE)

    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)
  }
}

# Apply function to "api_info"
lapply(api_info, pull_and_write)

# Cleanup
rm(list = ls())