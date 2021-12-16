library(aws.s3)
library(dplyr)
library(sf)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")


# INDUSTRIAL GROWTH ZONE
remote_file_industrial_growth_zone <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "industrial_growth_zone",
  paste0("2019", ".geojson")
)
tmp_file_industrial_growth_zone <- tempfile(fileext = ".geojson")

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_industrial_growth_zone)) {
  st_read(paste0(
    "https://opendata.arcgis.com/datasets/",
    "76e52da12b56406c945662eea968f3e1_1.geojson"
  )) %>%
    st_write(tmp_file_industrial_growth_zone, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_industrial_growth_zone, remote_file_industrial_growth_zone)
  file.remove(tmp_file_industrial_growth_zone)
}

# QUALIFIED OPPORTUNITY ZONE
remote_file_qualified_opportuniy_zone <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "qualified_opportuniy_zone",
  paste0("2019", ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_qualified_opportuniy_zone)) {
  tmp1 <- tempfile(fileext = ".zip")
  tmp2 <- tempfile()
  tmp3 <- tempfile(fileext = ".geojson")

  # Grab zipped shapefile - https://www.cdfifund.gov/opportunity-zones
  download.file(
    "https://www.cdfifund.gov/sites/cdfi/files/documents/opportunity-zones=8764.-9-10-2019.zip",
    destfile = tmp1
  )

  # Unzip shapefile
  unzip(tmp1, exdir = tmp2, overwrite = T)

  # Only keep Cook County, save as geojson
  st_read(grep("shp", list.files(tmp2, recursive = TRUE, full.names = TRUE), value = T)) %>%
    filter(STATENAME == "Illinois" & COUNTYNAME == "Cook") %>%
    st_write(tmp3)

  # Upload, clean
  aws.s3::put_object(tmp3, remote_file_qualified_opportuniy_zone)
  file.remove(c(tmp1, tmp3, list.files(tmp2, full.names = TRUE)))
}
