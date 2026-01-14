library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(stringr)
library(tools)
source("utils.R")

################################################################################
# This is static data and this script does not need to be re-run unless the data
# is no longer availale in the Data Department's warehouse S3 bucket.
################################################################################

# This script cleans and uploads condo parking space data for the warehouse.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "condominium", "pin_nonlivable"
)

files <- aws.s3::get_bucket_df(
  AWS_S3_RAW_BUCKET,
  prefix = "ccao/condominium/"
) %>%
  filter(stringr::str_ends(Key, ".parquet") & !str_detect(Key, "char")) %>%
  mutate(Key = file.path(AWS_S3_RAW_BUCKET, Key)) %>%
  pull(Key)

nonlivable <- list()

##### QUESTIONABLE GARAGE UNITS #####

nonlivable[["questionable"]] <- read_parquet(
  grep("questionable", files, value = TRUE)
) %>%
  filter(!str_detect(X3, "storage") | is.na(X3)) %>%
  mutate(year = "2022") %>%
  select("pin" = 1, "year") %>%
  filter(str_detect(pin, "^[:digit:]+$")) %>%
  mutate(
    pin = str_pad(pin, 14, side = "left", pad = "0"),
    flag = "questionable"
  )

##### 399 GARAGE UNITS #####

nonlivable[["gr_399s"]] <- read_parquet(grep("399", files, value = TRUE)) %>%
  select("pin" = "PARID", "year" = "TAXYR") %>%
  mutate(
    across(.cols = everything(), ~ as.character(.x)),
    flag = "399 GR"
  )

##### NEGATIVE PREDICTED VALUES #####

nonlivable[["neg_pred"]] <- map(
  grep("negative", files, value = TRUE), function(x) {
    read_parquet(x) %>%
      mutate(across(.cols = everything(), ~ as.character(.x))) %>%
      mutate(
        pin = str_remove_all(pin, "-"),
        year = str_extract(x, "[0-9]{4}"),
        flag = "negative pred"
      )
  }
) %>%
  bind_rows()

# Upload all nonlivable spaces to nonlivable table
nonlivable %>%
  bind_rows() %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = output_bucket,
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
