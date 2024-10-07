library(aws.s3)
library(dplyr)
library(sf)
library(geoarrow)

# Define the S3 bucket and file path
AWS_S3_RAW_BUCKET <- "ccao-data-raw-us-east-1"
file_key <- "spatial/environment/traffic/2023.parquet"

# Pipeline: download, read, and process the data with lowercase column names
shapefile_data <- tempfile(fileext = ".parquet") %>%
  {save_object(object = file_key, bucket = AWS_S3_RAW_BUCKET, file = .); .} %>%
  geoarrow::read_geoparquet() %>%
  mutate(geometry = st_as_sfc(geometry)) %>%
  st_as_sf() %>%
  st_transform(4326) %>%
  mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
  select(all_of(intersect(c("lns", "surf_typ", "surf_wth", "srf_yr", "aadt", "crs_with", "crs_opp", "crs_yr",
                            "road_name", "dtress_wth", "dtress_opp", "sp_lim", "inventory", "geometry_3435"),
                          tolower(colnames(.)))))
