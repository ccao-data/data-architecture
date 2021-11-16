library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(readr)
library(sf)
library(sfarrow)
library(stringr)
library(tictoc)
library(tidyr)

# This script cleans historical Cook County parcel data and uploads it to S3
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Get list of all parcel files (geojson AND attribute files) in the raw bucket
parcel_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "parcel")
) %>%
  filter(Size > 0) %>%
  mutate(
    year = str_extract(Key, "[0-9]{4}"),
    s3_uri = file.path(AWS_S3_RAW_BUCKET, Key),
    type = ifelse(str_detect(s3_uri, "geojson"), "spatial", "attr")
  ) %>%
  select(year, s3_uri, type) %>%
  pivot_wider(names_from = type, values_from = s3_uri)


# Save S3 parcel and attribute files locally for loading with sf
save_local_parcel_files <- function(year, spatial_uri, attr_uri) {
  temp_dir <- file.path(dirname(tempdir()), "parcel")
  tmp_file_spatial <- file.path(temp_dir, paste0(year, ".geojson"))
  tmp_file_attr <- file.path(temp_dir, paste0(year, "-attr.parquet"))
  if (!file.exists(tmp_file_spatial)) {
    print(paste("Grabbing geojson file for:", year))
    aws.s3::save_object(spatial_uri, file = tmp_file_spatial)
  }
  if (!file.exists(tmp_file_attr)) {
    print(paste("Grabbing attribute file for:", year))
    aws.s3::save_object(attr_uri, file = tmp_file_attr)
  }
}


# Load local parcel file, clean, extract centroids, and write to partitioned
# dataset on S3
process_parcel_file <- function(row) {
  file_year <- row["year"]
  attr_uri <- row["attr"]
  spatial_uri <- row["spatial"]
  tictoc::tic(paste("Finished processing parcel file for:", file_year))

  # Download S3 files to local temp dir if the don't exist
  save_local_parcel_files(file_year, spatial_uri, attr_uri)

  # Local file paths for parcel files
  parcel_dir <- file.path(dirname(tempdir()), "parcel")
  local_spatial_file <- file.path(parcel_dir, paste0(file_year, ".geojson"))
  local_attr_file <- file.path(parcel_dir, paste0(file_year, "-attr.parquet"))
  local_backup_file <- file.path(parcel_dir, paste0(file_year, "-proc.parquet"))

  # Only run processing if local backup doesn't exist
  if (!file.exists(local_backup_file)) {
    print(paste("Now processing parcel file for:", file_year))

    # Read local geojson file
    tictoc::tic(paste("Read file for:", file_year))
    spatial_df_raw <- st_read(local_spatial_file)
    tictoc::toc()

    # If PIN14 is missing, create a column by padding PIN10
    if (!"pin14" %in% names(spatial_df_raw)) {
      spatial_df_raw <- spatial_df_raw %>%
        rename_with(tolower) %>%
        mutate(pin14 = str_pad(pin10, 14, "left", "0"))
    }

    # Clean up spatial data of local file
    tictoc::tic(paste("Cleaned file for:", file_year))
    spatial_df_clean <- spatial_df_raw %>%
      filter(!is.na(pin10)) %>%
      # Keep only vital columns
      rename_with(tolower) %>%
      select(pin14, pin10, geometry) %>%
      mutate(
        # Drop any non-numeric chars from PINs
        across(c(pin10, pin14), ~ gsub("\\D", "", .x)),
        # Ensure PINs are zero-padded
        pin10 = str_pad(pin10, 10, "left", "0"),
        pin14 = str_pad(pin14, 14, "left", "0"),
        # If PIN10 is missing, fill with PIN14
        pin10 = ifelse(is.na(pin10), str_sub(pin14, 1, 10), pin10)
      ) %>%
      # Ensure valid geometry and convert to 4326 if not already
      st_make_valid() %>%
      st_transform(3435) %>%

      # Get the centroid of each polygon
      cbind(
        st_coordinates(st_transform(st_centroid(.), 4326)),
        st_coordinates(st_centroid(.))
      ) %>%
      mutate(area = st_area(.)) %>%
      rename(lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1) %>%
      st_transform(4326) %>%

      # For each PIN10, keep the centroid of the largest polygon and then union
      # of all geometries/shapes associated with that PIN10 into one multipolygon
      group_by(pin10) %>%
      arrange(desc(area)) %>%
      summarize(
        lon = first(lon),
        lat = first(lat),
        x_3435 = first(x_3435),
        y_3435 = first(y_3435),
        geometry = st_union(geometry)
      ) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435))
    tictoc::toc()

    # Read attribute data and get unique attributes by PIN10
    attr_df <- read_parquet(local_attr_file) %>%
      mutate(pin10 = str_sub(pin, 1, 10)) %>%
      group_by(pin10) %>%
      summarize(
        tax_code = first(tax_code),
        nbhd_code = first(nbhd_code),
        town_code = first(town_code),
        year = file_year
      )

    # Merge spatial boundaries with attribute data
    spatial_df_merged <- spatial_df_clean %>%
      left_join(attr_df, by = "pin10") %>%
      mutate(has_attributes = !is.na(town_code)) %>%
      select(
        pin10, tax_code, nbhd_code, has_attributes,
        lon, lat, x_3435, y_3435, geometry, geometry_3435,
        town_code, year
      )

    # Write local backup copy
    st_write_parquet(spatial_df_merged, local_backup_file)

  } else {
    print(paste("Loading processed parcels from backup for:", file_year))
    spatial_df_merged <- st_read_parquet(local_backup_file)
  }

  # Write final dataframe to dataset on S3, partitioned by town and year
  spatial_df_merged %>%
    mutate(year = file_year) %>%
    group_by(year, town_code) %>%
    group_walk(~ {
      year <- replace_na(.y$year, "__HIVE_DEFAULT_PARTITION__")
      town_code <- replace_na(.y$town_code, "__HIVE_DEFAULT_PARTITION__")
      remote_path <- file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel",
        paste0("year=", year), paste0("town_code=", town_code),
        "part-0.parquet"
      )
      if (!object_exists(remote_path)) {
        print(paste("Now uploading:", year, "data for town:", town_code))
        tmp_file <- tempfile(fileext = ".parquet")
        st_write_parquet(.x, tmp_file, compression = "snappy")
        aws.s3::put_object(tmp_file, remote_path)
      }
    })
  tictoc::toc()
}

apply(parcel_files_df, 1, process_parcel_file)
