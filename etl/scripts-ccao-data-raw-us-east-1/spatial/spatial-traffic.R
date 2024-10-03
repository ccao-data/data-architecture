library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(arrow)

# Define S3 bucket and paths
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")
current_year <- strftime(Sys.Date(), "%Y")

# Function to process each year and upload shapefiles for that specific year to S3
process_shapefiles_for_year <- function(year) {
  # Define the URL for the shapefile ZIP file, dynamically for each year
  url <- paste0("https://apps1.dot.illinois.gov/gist2/gisdata/all", year, ".zip")

  # Create a temporary file to store the downloaded ZIP
  temp_zip <- tempfile(fileext = ".zip")
  temp_dir <- tempdir()

  # Use httr to download the ZIP file to a temporary location
  response <- httr::GET(url)

  # Check if the request was successful
  if (httr::status_code(response) == 200) {
    # Save the content of the response as a ZIP file in a temporary location
    writeBin(httr::content(response, "raw"), temp_zip)
    message(paste("Shapefile ZIP for year", year, "downloaded successfully."))

    # Unzip the file into a temporary directory
    utils::unzip(temp_zip, exdir = temp_dir)
    message(paste("Shapefile for year", year, "unzipped into temporary directory."))

    # List files in the unzipped directory and look for the .shp files
    unzipped_files <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)
    shp_file_for_year <- unzipped_files[grepl(paste0("T2HWY", year), unzipped_files, ignore.case = TRUE) & grepl("\\.shp$", unzipped_files)]

    # Process only the shapefile that matches the current year
    if (length(shp_file_for_year) == 1) {
      # Read the shapefile into the environment using sf::st_read
      shapefile_data <- sf::st_read(shp_file_for_year)

      # Create a temporary file to save the shapefile as GeoParquet for S3 upload
      temp_parquet <- tempfile(fileext = ".parquet")

      # Save the shapefile as a GeoParquet file
      sf::st_write_parquet(shapefile_data, temp_parquet)

      # Define remote file path in S3
      remote_file_path <- file.path(output_bucket, paste0("T2HWY_", year, ".parquet"))

      # Upload to S3 if it doesn't already exist
      if (!aws.s3::object_exists(remote_file_path)) {
        message(paste("Uploading T2HWY_", year, "to S3 as Parquet..."))
        put_object(file = temp_parquet, object = remote_file_path, bucket = AWS_S3_RAW_BUCKET)

        message(paste("Shapefile T2HWY", year, "uploaded to S3 at:", remote_file_path))
      } else {
        message(paste("Shapefile T2HWY", year, "already exists in S3, skipping upload."))
      }

      # Clean up temporary files
      file.remove(temp_parquet)

    } else {
      message(paste("No shapefile found for year", year, "."))
    }

  } else {
    message(paste("Failed to retrieve the file for year", year, ". Status code: ", httr::status_code(response)))
  }
}

# Loop through the years from 2012 to the current year and process each shapefile
for (year in 2012:current_year) {
  process_shapefiles_for_year(year)
}
