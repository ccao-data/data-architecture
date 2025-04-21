library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(tidyr)
source("utils.R")

# This script cleans a hedonic price index sourced from the DePaul Institute for
# Housing Studies
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "housing", "ihs_index")

# Location of file to clean
raw_file <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "housing/ihs_index/")$Key
)

# Just need to properly format PUMA and change data from wide to long
read_parquet(raw_file) %>%
  rename(geoid = puma) %>%
  mutate(geoid = gsub("p", "170", geoid)) %>%
  pivot_longer(
    cols = !c("geoid", "name"),
    names_to = c("year", "quarter"),
    names_sep = "Q",
    values_to = "ihs_index"
  ) %>%
  # Upload as partitioned parquet files
  relocate(year, .after = last_col()) %>%
  group_by(year) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
