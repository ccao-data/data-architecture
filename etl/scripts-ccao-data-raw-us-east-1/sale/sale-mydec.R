library(dplyr)
library(lubridate)
library(RSocrata)
library(tictoc)
source("utils.R")

# This script retrieves raw mydec data from Illinois Department of Revenue
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "sale", "mydec")

# Mydec sales data is available from IDOR's API. We grab data for all years
# because we're not entirely sure how common updates for previous years are.
# This API request can take a while.
tic("Querying IDOR API")
sales <- read.socrata(
  paste0(
    "https://data.illinois.gov/resource/it54-y4c6.json",
    "?$where=line_1_county=%27Cook%27"
  ),
  app_token = Sys.getenv("SOCRATA_APP_TOKEN")
)
toc()

sales %>%
  mutate(year = year(line_4_instrument_date)) %>%
  group_by(year) %>%
  write_partitions_to_s3(
    s3_output_path = output_bucket,
    is_spatial = FALSE,
    overwrite = TRUE
  )
