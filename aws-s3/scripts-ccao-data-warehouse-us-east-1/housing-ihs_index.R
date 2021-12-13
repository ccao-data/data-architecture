library(arrow)
library(aws.s3)
library(dplyr)
library(tidyr)

# This script retrieves raw DePaul IHS data for the data lake
# It assumes a couple things about the imported .xlsx:
# - Three unnamed columns renamed "X1", "X2", and "X3" by R and
# - The value of the first row/column being "YEARQ"
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Location of file to clean
raw_file <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'housing/ihs_index/')$Key
)

# Just need to properly format PUMA and change data from wide to long
read_parquet(raw_file) %>%
  rename(geoid = puma) %>%
  mutate(geoid = gsub("p", "170", geoid)) %>%
  pivot_longer(
    cols = !c('geoid', 'name'),
    names_to = c("year", "quarter"),
    names_sep = "Q",
    values_to = "ihs_index"
    ) %>%

  # Upload as partitioned parquet files
  relocate(year, .after = last_col()) %>%
  group_by(year) %>%
  group_walk(~ {
    year <- replace_na(.y$year, "__HIVE_DEFAULT_PARTITION__")
    remote_path <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "housing", "ihs_index",
      paste0("year=", year),
      "part-0.parquet"
    )
    if (!object_exists(remote_path)) {
      print(paste("Now uploading:", year))
      tmp_file <- tempfile(fileext = ".parquet")
      write_parquet(.x, tmp_file, compression = "snappy")
      aws.s3::put_object(tmp_file, remote_path)
    }
  })

# Cleanup
rm(list = ls())