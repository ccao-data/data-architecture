library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)
source("utils.R")

# This scripts cleans and uploads airport noise surface data
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "environment", "airport_noise"
)

# Location of files to clean
raw_files <- grep(
  "parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "spatial/environment/airport_noise"
    )$Key
  ),
  value = TRUE
)

clean_noise_surface <- function(raw_file) {
  year <- as.character(str_match(raw_file, "[0-9]{4}"))
  print(year)
  if (is.na(year)) year <- "omp"
  df <- st_read_parquet(raw_file) %>%
    select(
      noise_dnl_pred = var1.pred,
      noise_dnl_var = var1.var,
      geometry_3435 = geometry
    ) %>%
    st_write_parquet(
      file.path(output_bucket, paste0("year=", year), "part-0.parquet")
    )
}

purrr::walk(raw_files, clean_noise_surface)
