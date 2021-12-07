library(aws.s3)
library(dplyr)
library(sf)

# This script retrieves school district and attendance boundaries published by
# the various districts around Cook County
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

api_info <- list(
  # DISTRICTS - UNIT
  "districts_unit_2012" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "9pjs-siys?method=export&format=GeoJSON",
                            "boundary" = "school_district_unified",
                            "year"     = "2012"),

  "districts_unit_2013" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "6bgu-89x4?method=export&format=GeoJSON",
                            "boundary" = "school_district_unified",
                            "year"     = "2013"),

  "districts_unit_2014" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "239w-pnii?method=export&format=GeoJSON",
                            "boundary" = "school_district_unified",
                            "year"     = "2014"),

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
  "districts_elem_2012" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "6mxp-5tr3?method=export&format=GeoJSON",
                            "boundary" = "school_district_elementary",
                            "year"     = "2012"),

  "districts_elem_2013" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "y5p9-c2pm?method=export&format=GeoJSON",
                            "boundary" = "school_district_elementary",
                            "year"     = "2013"),

  "districts_elem_2014" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "p5v4-ncme?method=export&format=GeoJSON",
                            "boundary" = "school_district_elementary",
                            "year"     = "2014"),

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
  "districts_scnd_2012" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "yh6i-3pdt?method=export&format=GeoJSON",
                            "boundary" = "school_district_secondary",
                            "year"     = "2012"),

  "districts_scnd_2013" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "d7n8-78xk?method=export&format=GeoJSON",
                            "boundary" = "school_district_secondary",
                            "year"     = "2013"),

  "districts_scnd_2014" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                            "api_url"  = "9zsm-c6ah?method=export&format=GeoJSON",
                            "boundary" = "school_district_secondary",
                            "year"     = "2014"),

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
  "attendance_ele_0607" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "qbay-3nnc?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2006-2007"),

  "attendance_ele_0708" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "r2h7-fxir?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2007-2008"),

  "attendance_ele_0809" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "8jx5-pt46?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2008-2009"),

  "attendance_ele_0910" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "sra3-5rba?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2009-2010"),

  "attendance_ele_1011" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "cjdh-ffxb?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2010-2011"),

  "attendance_ele_1112" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "6tkx-ju8g?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2011-2012"),

  "attendance_ele_1213" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "rfrd-v47v?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2012-2013"),

  "attendance_ele_1314" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "g7sv-g285?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2013-2014"),

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

  "attendance_ele_1819" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "whkz-sk6f?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2018-2019"),

  "attendance_ele_1920" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "abk6-gwwr?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2019-2020"),

  "attendance_ele_2021" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "gaak-qc7r?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_elementary",
                            "year"     = "2020-2021"),

  # CPS ATTENDANCE - SECONDARY
  "attendance_sec_0607" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "rwav-ux96?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2006-2007"),

  "attendance_sec_0708" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "wifq-a78y?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2007-2008"),

  "attendance_sec_0809" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "fede-88y6?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2008-2009"),

  "attendance_sec_0910" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "csqr-8bm8?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2009-2010"),

  "attendance_sec_1011" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "4ytu-3pkn?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2010-2011"),

  "attendance_sec_1112" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "s7sc-q48d?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2011-2012"),

  "attendance_sec_1213" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "9xc2-zddf?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2012-2013"),

  "attendance_sec_1314" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "azwk-fxgp?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2013-2014"),

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
                            "year"     = "2017-2018"),

  "attendance_sec_1819" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "bv6n-449d?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2018-2019"),

  "attendance_sec_1920" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "d95y-ue9h?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2019-2020"),

  "attendance_sec_2021" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                            "api_url"  = "da2c-wnfg?method=export&format=GeoJSON",
                            "boundary" = "cps_attendance_secondary",
                            "year"     = "2020-2021"),

  # LOCATION
  "locations_all_21" = c("source"   = "https://opendata.arcgis.com/datasets/",
                         "api_url"  = "a9a2e342397249fd90872765d11aede7_4.geojson",
                         "boundary" = "location",
                         "year"     = "2021")
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