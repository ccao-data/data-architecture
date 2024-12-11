library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script ingests data from First Street (firststreet.org) and uses results
# from their API to predict flood risk for each parcel
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())
input_bucket <- file.path(
  AWS_S3_RAW_BUCKET, "environment", "flood_first_street"
)
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "environment", "flood_first_street"
)

# Load First Street data directly from S3
flood_fs <- read_parquet(file.path(input_bucket, "2019.parquet")) %>%
  mutate(pin10 = str_sub(PIN, 1, 10), year = "2019") %>%
  distinct(pin10, .keep_all = TRUE) %>%
  select(-PIN)

# Load cleaned parcels to fill in missing flood data
parcels_df <- dbGetQuery(
  AWS_ATHENA_CONN_NOCTUA, glue(
    "SELECT pin10, x_3435, y_3435, year
  FROM spatial.parcel
  WHERE year = '2019'"
  )
)

# Merge FS data to parcel data
merged_df <- parcels_df %>%
  left_join(flood_fs, by = c("pin10", "year")) %>%
  st_as_sf(coords = c("x_3435", "y_3435"), crs = 3435)

# Split data into missing and non-missing frames
merged_missing_df <- merged_df %>%
  filter(is.na(fs_flood_factor))
merged_nonmissing_df <- merged_df %>%
  filter(!is.na(fs_flood_factor))

# Get the nearest feature from the non-missing frame
merged_fill <- merged_missing_df %>%
  mutate(nearest = st_nearest_feature(geometry, merged_nonmissing_df)) %>%
  st_drop_geometry() %>%
  cbind(., merged_nonmissing_df[.$nearest, ] %>%
    st_drop_geometry() %>%
    select(
      fill_fs_ff = fs_flood_factor,
      fill_fs_rd = fs_flood_risk_direction
    )) %>%
  select(-fs_flood_factor, -fs_flood_risk_direction)

# Merge filled data to initial dataframe
merged_final <- merged_df %>%
  st_drop_geometry() %>%
  left_join(merged_fill, by = c("pin10", "year")) %>%
  mutate(
    fs_flood_factor = ifelse(
      is.na(fs_flood_factor),
      fill_fs_ff,
      fs_flood_factor
    ),
    fs_flood_risk_direction = ifelse(
      is.na(fs_flood_risk_direction),
      fill_fs_rd,
      fs_flood_risk_direction
    )
  ) %>%
  select(pin10, fs_flood_factor, fs_flood_risk_direction, year) %>%
  arrange(year, pin10)

# Write final dataset to S3
merged_final %>%
  group_by(year) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
