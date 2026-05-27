# Uploads a list of PINs for which to obscure mailing names and addresses.
library(arrow)
library(dplyr)
library(readr)

# Declare output paths
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "other", "hidename"
)

read_csv("O:/CCAODATA/data/hidename/hidename.csv") %>%
  write_parquet(file.path(output_bucket, "hidename.parquet"))
