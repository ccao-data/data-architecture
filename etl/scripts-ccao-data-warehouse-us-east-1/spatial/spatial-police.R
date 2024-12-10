library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This scripts cleans and uploads boundaries for police districts
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "police")

# Location of file to clean
raw_files <- grep(
  "geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/police/")$Key
  ),
  value = TRUE
)

# Function to pull raw data from S3 and clean
clean_police <- function(remote_file) {
  year <- str_split(remote_file, "/", simplify = TRUE)[1, 7] %>%
    gsub(".geojson", "", .)

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file, file = tmp_file)

  return(
    st_read(tmp_file) %>%
      st_make_valid() %>%
      group_by(dist_num, dist_label) %>%
      summarise() %>%
      standardize_expand_geo(make_valid = FALSE) %>%
      mutate(year = year) %>%
      select(pd_num = dist_num, pd_name = dist_label, dplyr::everything())
  )

  file.remove(tmp_file)
}

# Apply function to raw_files
sapply(raw_files, clean_police, simplify = FALSE, USE.NAMES = TRUE) %>%
  bind_rows() %>%
  group_by(year) %>%
  write_partitions_to_s3(
    file.path(output_bucket, "police_district"),
    is_spatial = TRUE,
    overwrite = TRUE
  )

# Function to partition and upload cleaned data to S3
combine_upload <- function(economic_unit) {
  cleaned_output[grep(economic_unit, names(cleaned_output))] %>%
    bind_rows() %>%
    group_by(across(contains("name"))) %>%
    # Some shapefiles don't have consistent identifiers across time, create them using group IDs
    mutate(across(contains("num"), ~ case_when(
      is.na(.) ~ str_pad(cur_group_id(), width = 3, side = "left", pad = "0"),
      TRUE ~ .
    ))) %>%
    group_by(year) %>%
    write_partitions_to_s3(
      file.path(output_bucket, economic_unit),
      is_spatial = TRUE,
      overwrite = TRUE
    )
}
