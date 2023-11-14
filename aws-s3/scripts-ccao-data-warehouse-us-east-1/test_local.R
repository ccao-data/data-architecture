library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(glue)
library(noctua)
library(osmdata)
library(purrr)
library(sf)
source("utils.R")

# This script ingests data from the AWS raw bucket, simplifies the number of
# nodes for computational speed-up, and writes the resulting output to the
# AWS warehouse bucket

# Instantiate S3 bucket names
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")


# Iterate over the years
# Update the ingest and output paths for each year
ingest_file_2014 <- file.path(
  AWS_S3_RAW_BUCKET, "spatial",
  "environment", "major_road",
  "year=2014",
  "major_road-2014.parquet")

# Iterate over the years
# Update the ingest and output paths for each year
ingest_file_2017 <- file.path(
  AWS_S3_RAW_BUCKET, "spatial",
  "environment", "major_road",
  "year=2017",
  "major_road-2017.parquet")

# Iterate over the years
# Update the ingest and output paths for each year
ingest_file_2023 <- file.path(
  AWS_S3_RAW_BUCKET, "spatial",
  "environment", "major_road",
  "year=2023",
  "major_road-2023.parquet")


data_2014 <- read_geoparquet_sf(ingest_file_2014)
data_2017 <- read_geoparquet_sf(ingest_file_2017)
data_2023 <- read_geoparquet_sf(ingest_file_2023)



