library(aws.s3)
library(dplyr)
library(httr)
library(lubridate)
library(purrr)
library(sf)
library(arrow)

# Define S3 bucket and paths
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(
  AWS_S3_RAW_BUCKET,
  "spatial", "environment", "road"
)

# Get list of available files
years <- map(2012:year(Sys.Date()), \(x) {
  if (HEAD(paste0(
    "https://apps1.dot.illinois.gov/gist2/gisdata/all",
    x, ".zip"
  ))$status_code == 200) {
    x
  }
}) %>%
  unlist()

# Function to process each year and upload shapefiles for
# that specific year to S3
walk(years, \(x) {
  remote_file_path <- file.path(output_bucket, paste0(x, ".parquet"))

  # Skip everything if file already exists
  if (!object_exists(remote_file_path)) {
    # Define the URL for the shapefile ZIP file, dynamically for each year
    url <- paste0(
      "https://apps1.dot.illinois.gov/gist2/gisdata/all", x, ".zip"
    )

    # Create a temporary file to store the downloaded ZIP
    temp_zip <- tempfile(fileext = ".zip")
    temp_dir <- tempdir()

    # Download the ZIP file to a temporary location
    download.file(url = url, destfile = temp_zip)

    message(paste("Shapefile ZIP for year", x, "downloaded successfully."))

    # Unzip the file into a temporary directory
    unzip(temp_zip, exdir = temp_dir)
    message(paste(
      "Shapefile for year", x,
      "unzipped into temporary directory."
    ))

    # List files in the unzipped directory and look for the .shp files
    unzipped_files <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)
    shp_file_for_year <- unzipped_files[grepl(
      paste0(
        "HWY",
        x
      ),
      unzipped_files,
      ignore.case = TRUE
    ) &
      grepl("\\.shp$", unzipped_files)]

    # Process only the shapefile that matches the current year
    if (length(shp_file_for_year) == 1) {
      # Read the shapefile into the environment using sf::st_read
      shapefile_data <- sf::st_read(shp_file_for_year) %>%
        # Add filter for Cook County. The name changes in different years.
        filter(if ("COUNTY" %in% names(.)) {
          COUNTY == "016"
        } else {
          INV_CO == "016"
        }) %>%
        mutate(year = as.character(x))

      # Save the shapefile as a GeoParquet file
      geoarrow::write_geoparquet(shapefile_data, remote_file_path)
    } else {
      message(paste("No shapefile found for year", x, "."))
    }
  }
})
