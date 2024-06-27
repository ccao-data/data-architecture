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


# Function to calculate the interior angles of a polygon given its X and Y
# coordinates using the directions of the vectors between each pair of points
# See: https://stackoverflow.com/questions/12083480/finding-internal-angles-of-polygon
# Good example parcel for angle calc: 1418307019
calculate_angles <- function(points) {
  vectors <- diff(rbind(points, points[2, ])) * -1

  cross_product <- vectors[-nrow(vectors), 1] * vectors[-1, 2] -
    vectors[-1, 1] * vectors[-nrow(vectors), 2]
  dot_product <- vectors[-nrow(vectors), 1] * vectors[-1, 1] +
    vectors[-nrow(vectors), 2] * vectors[-1, 2]

  angles <- pi + atan2(cross_product, dot_product)
  angles <- c(NA_real_, angles * 180 / pi)

  return(angles)
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

    # Convert to a data.table of the vertices of each polygon. Using data.table
    # here because it's much faster
    spatial_mat_coords <- spatial_df_merged %>%
      st_set_geometry("geometry_3435") %>%
      st_coordinates() %>%
      as.data.table()

    # Use the vertices to calculate additional features about the shape of
    # the polygon
    spatial_mat_calc <- spatial_mat_coords[
      ,
      `:=` (
        # Get edge length using Pythagorean theorem and a rolling lag to get
        # the distance between each pair of points
        edge_len = sqrt(
          (X - data.table::shift(X, type = "lag")) ^ 2 +
            (Y - data.table::shift(Y, type = "lag")) ^ 2
        ),
        # Distance between the centroid and each point in the polygon
        dist_to_centroid = sqrt(
          (X - mean(X)) ^ 2 +
            (Y - mean(Y)) ^ 2
        ),
        # Calculate the interior angle between each pair of points
        angle = calculate_angles(as.matrix(.SD))
      ),
      .SDcols = c("X", "Y"),
      by = c("L1", "L2", "L3")
    ]

    # Collapse the vertex-level data back to the parcel level EXCLUDING any
    # vertices with an angle close to 180 degrees. These are likely to be
    # artifacts of the data and not true vertices, so we don't count them in
    # our calculations
    spatial_mat_calc_vert <- spatial_mat_calc[
      !data.table::between(angle, 179, 181) | is.na(angle),
      # Number of vertices in the polygon. Minus 1 because the first and last
      # vertex are always identical (used to close the polygon)
      .(
        shp_parcel_num_vertices = .N - 1,
        # Tail is used to drop the first row of each group since it's identical
        # to the last row (again, same reason as above)
        shp_parcel_interior_angle_sd = sd(tail(angle, -1), na.rm = TRUE),
        shp_parcel_centroid_dist_ft_sd = sd(tail(dist_to_centroid, -1))
      ),
      by = "L3"
    ]

    # Collapse the edge-level data back to the parcel level, this time keeping
    # the 180 degree vertices, since excluding them would drop some edges with
    # non-zero length
    spatial_mat_calc_edge <- spatial_mat_calc[
      ,
      .(
        shp_parcel_edge_len_ft_sd = sd(tail(edge_len, -1), na.rm = TRUE)
      ),
      by = "L3"
    ]

    # Calculate the ratio of the parcel area to the area of its minimum bounding
    # rectangle
    spatial_df_rec <- spatial_df_merged %>%
      st_drop_geometry() %>%
      st_set_geometry("geometry_3435") %>%
      mutate(parcel_area = st_area(geometry_3435)) %>%
      st_minimum_rotated_rectangle() %>%
      mutate(
        shp_parcel_mrr_area_ratio = units::drop_units(
          st_area(geometry_3435) / parcel_area
        )
      )

    # Calculate the ratio of the sides of the minimum bounding rectangle
    spatial_mat_rec_coords <- spatial_df_rec %>%
      st_coordinates() %>%
      as.data.table()

    spatial_mat_rec_calc <- spatial_mat_rec_coords[
      ,
      edge_len := sqrt(
        (X - data.table::shift(X, type = "lag")) ^ 2 +
          (Y - data.table::shift(Y, type = "lag")) ^ 2
      ),
      by = c("L1", "L2")
    ][
      ,
      .(
        shp_parcel_mrr_side_ratio = max(edge_len, na.rm = TRUE) /
          min(edge_len, na.rm = TRUE)
      ),
      by = "L2"
    ]

    # Merge all calculated features back into the main dataframe
    # then sort by year, town code, and PIN10 for better compression
    spatial_df_final <- spatial_df_merged %>%
      cbind(
        spatial_df_rec %>%
          select(shp_parcel_mrr_area_ratio) %>%
          st_drop_geometry(),
        spatial_mat_rec_calc[, 2],
        spatial_mat_calc_edge[, 2],
        spatial_mat_calc_vert[, 2:4]
      ) %>%
      ungroup() %>%
      arrange(year, town_code, pin10) %>%
      relocate(starts_with("shp_"), .after = "y_3435") %>%
      relocate(
        c(shp_parcel_centroid_dist_ft_sd, shp_parcel_interior_angle_sd),
        .after = "shp_parcel_edge_len_ft_sd"
      )

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
