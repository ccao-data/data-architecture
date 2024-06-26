library(arrow)
library(aws.s3)
library(data.table)
library(dplyr)
library(geoarrow)
library(here)
library(purrr)
library(readr)
library(sf)
library(stringr)
library(tictoc)
library(tidyr)
source("utils.R")

# This script cleans historical Cook County parcel data and uploads it to S3
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel")
parcel_tmp_dir <- here("parcel-tmp")

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
  tmp_file_spatial <- file.path(parcel_tmp_dir, paste0(year, ".geojson"))
  tmp_file_attr <- file.path(parcel_tmp_dir, paste0(year, "-attr.parquet"))
  if (!file.exists(tmp_file_spatial)) {
    message("Grabbing geojson file for: ", year)
    aws.s3::save_object(spatial_uri, file = tmp_file_spatial)
  }
  if (!file.exists(tmp_file_attr)) {
    message("Grabbing attribute file for: ", year)
    aws.s3::save_object(attr_uri, file = tmp_file_attr)
  }
}


# Load local parcel file, clean, extract centroids, and write to partitioned
# dataset on S3
process_parcel_file <- function(s3_bucket_uri,
                                file_year,
                                attr_uri,
                                spatial_uri) {
  tictoc::tic(paste("Finished processing parcel file for:", file_year))

  # Download S3 files to local temp dir if they don't exist
  save_local_parcel_files(file_year, spatial_uri, attr_uri)

  # Local file paths for parcel files
  local_spatial_file <- file.path(parcel_tmp_dir, paste0(file_year, ".geojson"))
  local_attr_file <- file.path(parcel_tmp_dir, paste0(file_year, "-attr.parquet"))
  local_backup_file <- file.path(parcel_tmp_dir, paste0(file_year, "-proc.parquet"))

  # Only run processing if local backup doesn't exist
  if (!file.exists(local_backup_file)) {
    message("Now processing parcel file for: ", file_year)

    # Read local geojson file
    tictoc::tic(paste("Read file for:", file_year))
    spatial_df_raw <- st_read(local_spatial_file)
    tictoc::toc()

    # Clean up raw data file, dropping empty/invalid geoms and fixing PINs
    tictoc::tic(paste("Cleaned file for:", file_year))
    if (!"pin14" %in% names(spatial_df_raw)) {
      spatial_df_clean <- spatial_df_raw %>%
        rename_with(tolower) %>%
        filter(!is.na(pin10), !st_is_empty(geometry), st_is_valid(geometry)) %>%
        select(pin10, geometry) %>%
        mutate(
          pin10 = gsub("\\D", "", pin10),
          pin10 = str_pad(pin10, 10, "left", "0"),
          pin14 = str_pad(pin10, 14, "right", "0")
        ) %>%
        st_cast("MULTIPOLYGON")
    } else {
      spatial_df_clean <- spatial_df_raw %>%
        rename_with(tolower) %>%
        filter(!is.na(pin10), !st_is_empty(geometry), st_is_valid(geometry)) %>%
        select(pin14, pin10, geometry) %>%
        mutate(
          across(c(pin10, pin14), ~ gsub("\\D", "", .x)),
          pin10 = str_pad(pin10, 10, "left", "0"),
          pin14 = str_pad(pin14, 14, "left", "0"),
          pin10 = ifelse(is.na(pin10), str_sub(pin14, 1, 10), pin10)
        ) %>%
        st_cast("MULTIPOLYGON")
    }
    tictoc::toc()

    # Get the centroid of the largest polygon for each parcel
    tictoc::tic(paste("Calculated centroids for:", file_year))
    spatial_df_centroids <- spatial_df_clean %>%
      # Ensure valid geometry and dump empty geometries
      st_make_valid() %>%
      filter(!st_is_empty(geometry)) %>%
      # Split any multipolygon parcels into multiple rows, one for each polygon
      # https://github.com/r-spatial/sf/issues/763
      st_cast("POLYGON", warn = FALSE) %>%
      # Transform to planar geometry then calculate centroids
      st_transform(3435) %>%
      mutate(centroid_geom = st_centroid(geometry)) %>%
      cbind(
        st_coordinates(st_transform(.$centroid_geom, 4326)),
        st_coordinates(.$centroid_geom)
      ) %>%
      mutate(area = st_area(geometry)) %>%
      select(pin10, lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1, area) %>%
      st_drop_geometry() %>%
      # For each PIN10, keep the centroid of the largest polygon
      group_by(pin10) %>%
      arrange(desc(area)) %>%
      summarize(across(c(lon, lat, x_3435, y_3435), first)) %>%
      ungroup()
    tictoc::toc()

    # Read attribute data and get unique attributes by PIN10
    tictoc::tic(paste("Joined and wrote parquet for:", file_year))
    attr_df <- read_parquet(local_attr_file) %>%
      mutate(pin10 = str_sub(pin, 1, 10)) %>%
      group_by(pin10) %>%
      summarize(
        across(c(tax_code, nbhd_code, town_code), first),
        year = file_year
      ) %>%
      ungroup()

    # Merge spatial boundaries with attribute data
    spatial_df_merged <- spatial_df_clean %>%
      left_join(attr_df, by = "pin10") %>%
      left_join(spatial_df_centroids, by = "pin10") %>%
      mutate(
        has_attributes = !is.na(town_code),
        geometry = st_transform(geometry, 4326),
        geometry_3435 = st_transform(geometry, 3435)
      ) %>%
      select(
        pin10, tax_code, nbhd_code, has_attributes,
        lon, lat, x_3435, y_3435, geometry, geometry_3435,
        town_code, year
      ) %>%
      distinct(pin10, .keep_all = TRUE)

    # If centroids are missing from join (invalid geom, empty, etc.)
    # fill them in with centroid of the full multipolygon
    if (any(is.na(spatial_df_merged$lon) | any(is.na(spatial_df_merged$x_3435)))) {
      # Calculate centroids for missing
      spatial_df_missing <- spatial_df_merged %>%
        filter(is.na(lon) | is.na(x_3435)) %>%
        mutate(centroid_geom = st_centroid(geometry_3435)) %>%
        select(-lon, -lat, -x_3435, -y_3435) %>%
        cbind(
          st_coordinates(st_transform(.$centroid_geom, 4326)),
          st_coordinates(.$centroid_geom)
        ) %>%
        rename(lon = X, lat = Y, x_3435 = X.1, y_3435 = Y.1) %>%
        select(-centroid_geom)

      # Merge missing centroids back into main data
      spatial_df_merged <- spatial_df_merged %>%
        filter(!is.na(lon) & !is.na(x_3435)) %>%
        bind_rows(spatial_df_missing)
    }

    # Sort by year, town code, and PIN10 for better compression
    spatial_df_merged <- spatial_df_merged %>%
      ungroup() %>%
      arrange(year, town_code, pin10)

    spatial_df_temp <- spatial_df_merged %>%
      sample_n(1000)

    spatial_mat_coords <- spatial_df_temp %>%
      st_set_geometry("geometry_3435") %>%
      st_coordinates() %>%
      as.data.table()

    spatial_mat_coords[, .(Xlen = X - shift(X, type = "lag")), by = L3]

    temp <- spatial_mat_coords[
      ,
      `:=` (
        n = .N - 1,
        # Distance between points using Pythagorean theorem. The first point in
        # each group (polygon) is the same as the last point, so calculating the
        # length between all points in a group does capture the length of all
        # edges
        len = sqrt(
          (X - shift(X, type = "lag")) ^ 2 +
            (Y - shift(Y, type = "lag")) ^ 2
        )
      ),
      by = c("L1", "L2", "L3")
    ]

    spatial_df_temp %>%
      mutate(
        shp_parcel_num_points = spatial_mat_coords[, .(n = .N), by = L3]$n - 1,
      )

    # st_minimum_rotated_rectangle

    # Write local backup copy
    write_geoparquet(spatial_df_final, local_backup_file)
    tictoc::toc()
  } else {
    message("Loading processed parcels from backup for: ", file_year)
    spatial_df_final <- read_geoparquet_sf(local_backup_file)
  }

  # Write final dataframe to dataset on S3, partitioned by town and year
  spatial_df_final %>%
    mutate(year = file_year) %>%
    group_by(year, town_code) %>%
    write_partitions_to_s3(s3_bucket_uri, is_spatial = TRUE, overwrite = FALSE)
  tictoc::toc()
}

calculate_angles <- function(points) {
  # Calculate vectors between consecutive points
  vectors <- diff(rbind(points, points[1, ]))

  # Calculate angles between vectors
  angles <- atan2(vectors[-1,2], vectors[-1,1]) - atan2(vectors[-length(vectors[,1]),2], vectors[-length(vectors[,1]),1])

  # Calculate cross product of vectors
  cross_product <- vectors[-1,1]*vectors[-length(vectors[,1]),2] - vectors[-1,2]*vectors[-length(vectors[,1]),1]

  # If cross product is negative, subtract angle from 360 degrees
  angles[cross_product < 0] <- 2*pi - angles[cross_product < 0]

  # Normalize angles to [0, 2*pi]
  angles <- ifelse(angles < 0, angles + 2*pi, angles)

  # Convert angles from radians to degrees
  angles <- angles * 180 / pi

  # Return angles
  return(angles)
}

# Apply function to all parcel files
pwalk(parcel_files_df, function(...) {
  df <- tibble::tibble(...)
  process_parcel_file(
    s3_bucket_uri = output_bucket,
    file_year = df$year,
    attr_uri = df$attr,
    spatial_uri = df$spatial
  )
})
