# Load required libraries
library(aws.s3)
library(dplyr)
library(purrr)
library(sf)

# Define S3 bucket and paths
AWS_S3_RAW_BUCKET <- "ccao-data-raw-us-east-1"
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
raw_bucket_prefix <- "spatial/environment/traffic/"
warehouse_bucket_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "traffic")

# List files from the raw bucket
raw_files <- get_bucket_df(bucket = AWS_S3_RAW_BUCKET, prefix = raw_bucket_prefix)


process_files_from_raw_bucket <- map(raw_files$Key, \(file_key) {

  # Skip if the file is not a .parquet file
  if (!grepl("\\.parquet$", file_key)) {
    message(paste("Skipping non-parquet file:", file_key))
    return(NULL)
  }

  # Download the file locally for inspection
  local_parquet_file <- tempfile(fileext = ".parquet")

  # Corrected: Pass only the bucket name and file key
  save_object(file = local_parquet_file, object = file_key, bucket = AWS_S3_RAW_BUCKET)

  # Read the parquet file using geoarrow
  shapefile_data <- geoarrow::read_geoparquet(local_parquet_file)

  # Define the columns you want to select. These change over time, so a strict select isn't great.
  # But all columns are present from 2014 on.
  required_columns <- c("LNS", "SURF_TYP", "SURF_WTH", "SRF_YR", "AADT", "CRS_WITH", "CRS_OPP", "CRS_YR",
                        "ROAD_NAME", "DTRESS_WTH", "DTRESS_OPP", "SP_LIM", "INVENTORY")

  # Select only the columns that exist in the dataset
  existing_columns <- intersect(required_columns, colnames(shapefile_data))
  selected_columns <- shapefile_data %>%
    select(all_of(existing_columns))

  # Show the first few rows of the selected columns for inspection
  print(paste("File:", file_key))
  print(head(selected_columns))

  # Clean up the temporary local file
  unlink(local_parquet_file)

})

