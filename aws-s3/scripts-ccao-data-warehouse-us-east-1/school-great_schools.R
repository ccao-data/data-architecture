library(sfarrow)
library(dplyr)
library(aws.s3)
library(arrow)

# This script cleans data retrieved from greatschools.org
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

source_file <- file.path(
  AWS_S3_RAW_BUCKET,
  "school",
  "great_schools",
  paste0(format(Sys.Date(), "%Y"), ".parquet")
)

destination_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial",
  "school",
  "great_schools",
  paste0(format(Sys.Date(), "%Y"), ".parquet")
)

# Read, write data if it does not already exist
if (!aws.s3::object_exists(destination_file)) {

  # Pull from S3, convert to spatial object with lat/long 3435 CRS
  read_parquet(aws.s3::get_object(source_file)) %>%
    sf::st_as_sf(coords = c("lon", "lat"), remove = FALSE, crs = 4326) %>%
    sf::st_transform(3435) %>%
    mutate(county = "Cook",
           `district-id` = na_if(`district-id`, 0)) %>%
    select(-distance) %>%

    # Write to S3
    sfarrow::st_write_parquet(destination_file)

}