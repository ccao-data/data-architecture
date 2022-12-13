library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

# This script retrieves the boundaries of various Cook County taxing districts
# and entities, such as TIFs, libraries, etc.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "tax")

sources_list <- bind_rows(list(
  # TIF
  "tif_2015" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "si4i-nrtg?method=export&format=GeoJSON",
    "boundary" = "tif_district",
    "year" = "2015"
  ),
  "tif_2016" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "5dup-xpsj?method=export&format=GeoJSON",
    "boundary" = "tif_district",
    "year" = "2016"
  ),
  "tif_2018" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "cc516b88a47547bd974bba4ebc120ecf_15.geojson",
    "boundary" = "tif_district",
    "year" = "2018"
  ),
  "tif_2019" = c(
    "source" = "https://gis.cookcountyil.gov/traditional/rest/services/clerkTaxDistricts/MapServer/",
    "api_url" = "18/query?outFields=*&where=1%3D1&f=geojson",
    "boundary" = "tif_district",
    "year" = "2019"
  ),

  # LIBRARY
  "lib_2015" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "nu5w-d9cb?method=export&format=GeoJSON",
    "boundary" = "library_district",
    "year" = "2015"
  ),
  "lib_2016" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "junp-4s2h?method=export&format=GeoJSON",
    "boundary" = "library_district",
    "year" = "2016"
  ),
  "lib_2018" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "5d02a289bd774880a49b03e9ed16fc29_7.geojson",
    "boundary" = "library_district",
    "year" = "2018"
  ),

  # PARK
  "prk_2015" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "r43b-5ipg?method=export&format=GeoJSON",
    "boundary" = "park_district",
    "year" = "2015"
  ),
  "prk_2016" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "2df4-kwbu?method=export&format=GeoJSON",
    "boundary" = "park_district",
    "year" = "2016"
  ),
  "prk_2018" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "b9e46f08fde04125a9d225da9b1e33f6_11.geojson",
    "boundary" = "park_district",
    "year" = "2018"
  ),

  # FIRE PROTECTION
  "frp_2015" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "2rg9-v9k5?method=export&format=GeoJSON",
    "boundary" = "fire_protection_district",
    "year" = "2015"
  ),
  "frp_2016" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "egxy-cjyk?method=export&format=GeoJSON",
    "boundary" = "fire_protection_district",
    "year" = "2016"
  ),
  "frp_2018" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "8250672861de4690a6602113376015c9_3.geojson",
    "boundary" = "fire_protection_district",
    "year" = "2018"
  ),

  # COMMUNITY COLLEGE
  "ccl_2012" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "pt2x-hzk3?method=export&format=GeoJSON",
    "boundary" = "community_college_district",
    "year" = "2012"
  ),
  "ccl_2013" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "4byp-2m3p?method=export&format=GeoJSON",
    "boundary" = "community_college_district",
    "year" = "2013"
  ),
  "ccl_2014" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "uxmj-ssxr?method=export&format=GeoJSON",
    "boundary" = "community_college_district",
    "year" = "2014"
  ),
  "ccl_2015" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "h5ph-eevy?method=export&format=GeoJSON",
    "boundary" = "community_college_district",
    "year" = "2015"
  ),
  "ccl_2016" = c(
    "source" = "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "api_url" = "69tz-shqi?method=export&format=GeoJSON",
    "boundary" = "community_college_district",
    "year" = "2016"
  ),

  # SANITATION
  "san_2018" = c(
    "source" = "https://opendata.arcgis.com/datasets/",
    "api_url" = "b8cce49b653f4a059d527b0882f9667c_12.geojson",
    "boundary" = "sanitation_district",
    "year" = "2018"
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


##### SSAs #####
remote_file_ssa <- file.path(
  output_bucket, "special_service_area", "2020.geojson"
)
tmp_file_ssa <- "O:/CCAODATA/data/spatial/SpecServTaxDist_2020.geojson"
save_local_to_s3(remote_file_ssa, tmp_file_ssa)