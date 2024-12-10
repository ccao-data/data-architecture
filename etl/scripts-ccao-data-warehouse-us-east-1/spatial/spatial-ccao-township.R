library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(here)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans CCAO-specific shapefiles for townships and neighborhoods
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "ccao")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "ccao")


##### TOWNSHIP #####
remote_file_town_raw <- file.path(
  input_bucket, "township", "2022.geojson"
)
remote_file_town_warehouse <- file.path(
  output_bucket, "township", "2022.parquet"
)



if (!aws.s3::object_exists(remote_file_town_warehouse)) {
  tmp_file_town <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_town_raw, file = tmp_file_town)

  st_read(tmp_file_town) %>%
    st_transform(4326) %>%
    select("township_name" = "NAME", "geometry") %>%
    filter(township_name != "") %>%
    mutate(
      township_name = str_to_title(township_name),
      township_name = case_when(
        township_name %in% c("North", "South", "West") ~ paste(township_name, "Chicago"),
        TRUE ~ township_name
      )
    ) %>%
    left_join(ccao::town_dict %>% select("township_name", "township_code", "triad_name", "triad_code")) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435),
      across(township_code:triad_code, as.character)
    ) %>%
    mutate(loaded_at = as.character(Sys.time())) %>%
    geoarrow::write_geoparquet(remote_file_town_warehouse)
}
