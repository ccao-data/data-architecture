library(dplyr)
library(readr)
library(stringr)
source("utils.R")

# Declare output paths
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "other", "hidename"
)

read_csv("O:/CCAODATA/data/hidename/hidename.csv") %>%
  save_local_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
