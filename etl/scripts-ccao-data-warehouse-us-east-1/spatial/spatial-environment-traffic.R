library(aws.s3)
library(dplyr)
library(sf)
library(geoarrow)

# Define the S3 bucket and folder path
AWS_S3_RAW_BUCKET <- "ccao-data-raw-us-east-1"
AWS_S3_WAREHOUSE_BUCKET <- "ccao-data-warehouse-us-east-1"
s3_folder <- "spatial/environment/traffic/"
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment", "traffic")

files_in_s3 <- get_bucket_df(bucket = AWS_S3_RAW_BUCKET, prefix = s3_folder)

# Get the 'Key'
parquet_files <- files_in_s3 %>%
  pull(Key)

# Loop through each parquet file and process it
for (file_key in parquet_files) {

  # Read the parquet file directly from S3 using aws.s3 functions
  obj <- get_object(object = file_key, bucket = AWS_S3_RAW_BUCKET)

  # Convert the S3 object into raw data and read using geoarrow
  shapefile_data <- geoarrow::read_geoparquet(rawConnection(obj))

  # Convert geometry column to 'sf' format
  shapefile_data$geometry <- st_as_sfc(shapefile_data$geometry)

  shapefile_data <- shapefile_data %>%
    st_as_sf() %>%
    st_transform(4326) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435))

  # Define the columns you want to select. We do this because some columns are not present in older
  # versions of the data
  required_columns <- c("LNS", "SURF_TYP", "SURF_WTH", "SRF_YR", "AADT", "CRS_WITH", "CRS_OPP", "CRS_YR",
                        "ROAD_NAME", "DTRESS_WTH", "DTRESS_OPP", "SP_LIM", "INVENTORY", "geometry_3435")

  # Select only the non-geometry columns that exist in the dataset
  existing_columns <- intersect(required_columns, colnames(shapefile_data))
  selected_columns <- shapefile_data %>%
    select(all_of(existing_columns))

  # Create a temporary file for saving the processed data
  output_file <- tempfile(fileext = ".parquet")

  # Write the selected columns to a new parquet file
  geoarrow::write_geoparquet(selected_columns, output_file)

  # Define the output file path in the S3 bucket
  output_key <- file.path(output_bucket, basename(file_key))

  # Upload the processed file to the S3 output bucket
  put_object(file = output_file, object = output_key, bucket = AWS_S3_WAREHOUSE_BUCKET)

  # Clean up the temporary files
  unlink(output_file)
}
