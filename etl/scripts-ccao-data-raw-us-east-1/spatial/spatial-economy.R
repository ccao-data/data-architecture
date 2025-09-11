library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "economy")
current_year <- strftime(Sys.Date(), "%Y")

##### ENTERPRISE ZONE #####
remote_file_enterprise_zone <- file.path(
  output_bucket, "enterprise_zone",
  paste0("2021", ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_enterprise_zone)) {
  tmp_file_enterprise_zone <- tempfile(fileext = ".geojson")
  download.file(
    paste0(
      "https://data.cityofchicago.org/api/geospatial",
      "/bwpt-y235?method=export&format=GeoJSON"
    ),
    tmp_file_enterprise_zone
  )
  save_local_to_s3(
    remote_file_enterprise_zone,
    tmp_file_enterprise_zone
  )
  file.remove(tmp_file_enterprise_zone)
}

##### INDUSTRIAL GROWTH ZONE #####
remote_file_industrial_growth_zone <- file.path(
  output_bucket, "industrial_growth_zone",
  paste0("2019", ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_industrial_growth_zone)) {
  tmp_file_industrial_growth_zone <- tempfile(fileext = ".geojson")
  download.file(
    paste0(
      "https://opendata.arcgis.com/datasets/",
      "76e52da12b56406c945662eea968f3e1_1.geojson"
    ),
    tmp_file_industrial_growth_zone
  )
  save_local_to_s3(
    remote_file_industrial_growth_zone,
    tmp_file_industrial_growth_zone
  )
  file.remove(tmp_file_industrial_growth_zone)
}


##### QUALIFIED OPPORTUNITY ZONE #####
remote_file_qualified_opportunity_zone <- file.path(
  output_bucket, "qualified_opportunity_zone",
  paste0("2019", ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_qualified_opportunity_zone)) {
  tmp1 <- tempfile(fileext = ".zip")
  tmp2 <- tempfile()
  tmp3 <- tempfile(fileext = ".geojson")

  # Grab zipped shapefile - https://www.cdfifund.gov/opportunity-zones
  download.file(
    paste0(
      "https://www.cdfifund.gov/sites/cdfi/files/",
      "documents/opportunity-zones=8764.-9-10-2019.zip"
    ),
    destfile = tmp1
  )

  # Unzip shapefile
  unzip(tmp1, exdir = tmp2, overwrite = TRUE)

  # Only keep Cook County, save as geojson
  st_read(grep(
    "shp",
    list.files(tmp2, recursive = TRUE, full.names = TRUE),
    value = TRUE
  )) %>%
    filter(STATENAME == "Illinois" & COUNTYNAME == "Cook") %>%
    st_write(tmp3)

  # Upload, clean
  save_local_to_s3(remote_file_qualified_opportunity_zone, tmp3)
  file.remove(c(tmp1, tmp3, list.files(tmp2, full.names = TRUE)))
}

##### CENTRAL BUSINESS DISTRICT
remote_file_central_business_district <- file.path(
  output_bucket, "central_business_district",
  paste0(2016, ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_central_business_district)) {
  tmp_file_central_business_district <- tempfile(fileext = ".geojson")
  download.file(
    paste0(
      "https://data.cityofchicago.org/api/geospatial/",
      "tksj-nvsw?method=export&format=GeoJSON"
    ),
    tmp_file_central_business_district
  )
  save_local_to_s3(
    remote_file_central_business_district,
    tmp_file_central_business_district
  )
  file.remove(tmp_file_central_business_district)
}
