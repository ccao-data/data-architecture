library(arrow)
library(aws.s3)
library(dplyr)
library(tidyr)
library(stringr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "rpie", "pin_codes")

# Get a list of all district-level boundaries
# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "rpie/pin_codes/")$Key
  ),
  value = TRUE
)

add_year <- function(file) {

  return(
    read_parquet(file) %>%
      mutate(year = str_split(file, "=|\\/part")[[1]][2])
  )

}

map(files, add_year) %>%
  bind_rows() %>%
  rename(pin = RPIE_PIN,
         rpie_code = RPIE_CODE) %>%
  group_by(year) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
