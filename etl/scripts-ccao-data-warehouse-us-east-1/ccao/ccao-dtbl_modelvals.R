library(arrow)
library(dplyr)
source("utils.R")

################################################################################
# This static data and this script does not need to be re-run unless the data
# is no longer availale in the Data Department's warehouse S3 bucket.
################################################################################

# This script uploads archived model values from the data department's old SQL
# server
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "other", "dtbl_modelvals"
)

# Upload values to warehouse S3 bucket
read_parquet(
  "O:/CCAODATA/SQL_archives/CCAOREPORTSRV/DTBL_MODELVALS.parquet"
) %>%
  select(-row_names) %>%
  group_by(TAX_YEAR) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = FALSE)
