library(aws.s3)
library(dplyr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

# This script retrieves major political boundaries such as townships and
# judicial districts and keeps only necessary columns for reporting and analysis
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Location of file to clean
raw_files <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'spatial/political/')$Key
)

dest_files <- gsub(
  "geojson", "parquet",
  file.path(
    AWS_S3_WAREHOUSE_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'spatial/political/')$Key
  )
)

column_names <- c("board_of_review" = "district_n",
                  "commissioner" = "district",
                  "congressional_district" = "district_n",
                  "judicial" = "district",
                  "municipality" = "MUNICIPALITY",
                  "state_representative" = "district_n",
                  "state_senate" = "senatedist",
                  "township" = "NAME",
                  "ward" = "ward")

# Function to pull raw data from S3 and clean
clean_politics <- function(remote_file, dest_file) {

  political_unit <- str_split(remote_file, "/", simplify = TRUE)[1,6]

  if (!aws.s3::object_exists(dest_file)) {

    tmp_file <- tempfile(fileext = ".geojson")
    aws.s3::save_object(remote_file, file = tmp_file)

    st_read(tmp_file) %>%
      mutate_at(vars(contains('MUNICIPALITY')), replace_na, "Unincorporated") %>%
      select(column_names[political_unit], geometry) %>%
      mutate(across(where(is.character), str_to_title),
             geometry_3435 = st_transform(geometry, 3435)) %>%
      sfarrow::st_write_parquet(dest_file)

    file.remove(tmp_file)

  }

}

# Apply function to raw_files
mapply(clean_politics, raw_files, dest_files)

 # Cleanup
rm(list = ls())