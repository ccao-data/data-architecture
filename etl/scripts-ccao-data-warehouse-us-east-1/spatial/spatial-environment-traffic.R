library(aws.s3)
library(dplyr)
library(sf)
library(geoarrow)

# Define the S3 bucket and folder path
AWS_S3_RAW_BUCKET <- "ccao-data-raw-us-east-1"
AWS_S3_WAREHOUSE_BUCKET <- "ccao-data-warehouse-us-east-1"
s3_folder <- "spatial/environment/traffic/"
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "traffic")

# List all the files in the S3 folder
files_in_s3 <- get_bucket_df(bucket = AWS_S3_RAW_BUCKET, prefix = s3_folder)

# Filter for files that match a 'parquet' pattern
parquet_files <- files_in_s3 %>%
  filter(grepl("\\.parquet$", Key)) %>%
  pull(Key)

# Loop through each parquet file and process it
for (file_key in parquet_files) {
  message("Processing file: ", file_key)

  # Download the file from S3 as a raw connection into a temporary file
  temp_file <- tempfile(fileext = ".parquet")
  save_object(object = file_key, bucket = AWS_S3_RAW_BUCKET, file = temp_file)

  # Read the downloaded file using geoarrow into the R environment
  shapefile_data <- geoarrow::read_geoparquet(temp_file)

  # Ensure geometry column is in 'sf' format
  shapefile_data$geometry <- st_as_sfc(shapefile_data$geometry)

  shapefile_data <- shapefile_data %>%
    st_as_sf() %>%
    st_transform(4326) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435))

  # Define the columns you want to select
  required_columns <- c("LNS", "SURF_TYP", "SURF_WTH", "SRF_YR", "AADT", "CRS_WITH", "CRS_OPP", "CRS_YR",
                        "ROAD_NAME", "DTRESS_WTH", "DTRESS_OPP", "SP_LIM", "INVENTORY", "geometry_3435")

  # Select only the non-geometry columns that exist in the dataset
  existing_columns <- intersect(required_columns, colnames(shapefile_data))
  selected_columns <- shapefile_data %>%
    select(all_of(existing_columns))

  # Clean up the temporary file
  unlink(temp_file)
}

message("Processing completed for all files.")
