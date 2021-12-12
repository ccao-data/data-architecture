library(aws.s3)
library(dplyr)
library(sf)
library(stringr)

# This script retrieves school district and attendance boundaries published by
# the various districts around Cook County
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

api_info <- list(
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
                            "api_url"  = "7jn2-4muy?method=export&format=GeoJSON",
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


##### County-provided District Files #####

# This geodatabase was provided via Cook County BoT/GIS on 12/9/2021 and contains
# all school district boundaries from 2000 - 2020
gdb_file <- "O:/CCAODATA/data/SchoolTaxDist.gdb"
layers <- st_layers(gdb_file)$name


# Function to read each layer and save it to S3
process_layer <- function(layer) {
  dist_type <- str_sub(layer, 1, 4)
  dist_type <- recode(
    dist_type,
    Elem = "elementary",
    High = "secondary",
    Unit = "unified"
  )
  year <- str_match(layer, "[0-9]{4}")
  tmp_file <- tempfile(fileext = ".geojson")
  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "spatial", "school",
    paste0("school_district_", dist_type), paste0(year, ".geojson")
  )
  if (!aws.s3::object_exists(remote_file)) {

    st_read(gdb_file, layer) %>%
      st_write(tmp_file, delete_dsn = TRUE)

    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)
  }
}

lapply(layers, process_layer)
