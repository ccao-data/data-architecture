library(arrow)
library(aws.s3)
library(ccao)
library(data.table)
library(dplyr)
library(geoarrow)
library(here)
library(noctua)
library(purrr)
library(readr)
library(sf)
library(stringr)
library(tictoc)
library(tidyr)
library(tidygeocoder)
source("utils.R")

noctua_options(unload = TRUE)

# This script cleans historical Cook County parcel data and uploads it to S3
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel_test")
parcel_tmp_dir <- here("parcel-tmp")
con <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

# PARCEL CLEANING ----

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
# See: https://stackoverflow.com/a/12090743
# Good example parcel for angle calc: 1418307019
calculate_angles <- function(points) {
  vectors <- diff(rbind(points, points[2, ])) * -1

  cross_product <- vectors[-nrow(vectors), 1] * vectors[-1, 2] -
    vectors[-1, 1] * vectors[-nrow(vectors), 2]
  dot_product <- vectors[-nrow(vectors), 1] * vectors[-1, 1] +
    vectors[-nrow(vectors), 2] * vectors[-1, 2]

  angles <- pi + atan2(cross_product, dot_product)
  angles <- c(NA_real_, angles * 180 / pi)

  angles
}


# Load local parcel file, clean, extract centroids, and write to partitioned
# dataset on S3
process_parcel_file <- function(s3_bucket_uri,
                                file_year,
                                attr_uri,
                                spatial_uri) {
  file_year_processed <- aws.s3::get_bucket_df(
    bucket = AWS_S3_WAREHOUSE_BUCKET,
    prefix = file.path("spatial", "parcel_test")
  ) %>%
    filter(Size > 0, str_detect(Key, file_year), !str_detect(Key, "test2"))

  # Skip processing if expected data already exists in warehouse
  if (nrow(file_year_processed) != 39) {
    tictoc::tic(paste("Finished processing parcel file for:", file_year))

    # Download S3 files to local temp dir if they don't exist
    save_local_parcel_files(file_year, spatial_uri, attr_uri)

    # Local file paths for parcel files
    local_spatial_file <- file.path(
      parcel_tmp_dir, paste0(file_year, ".geojson")
    )
    local_attr_file <- file.path(
      parcel_tmp_dir, paste0(file_year, "-attr.parquet")
    )
    local_backup_file <- file.path(
      parcel_tmp_dir, paste0(file_year, "-proc.parquet")
    )

    # Only run processing if local backup doesn't exist
    if (!file.exists(local_backup_file)) {
      message("Now processing parcel file for: ", file_year)

      # Read local geojson file
      tictoc::tic(paste("Read file for:", file_year))
      spatial_df_raw <- st_read(local_spatial_file)
      tictoc::toc()

      # Clean up raw data file, dropping empty/invalid geoms and fixing PINs
      tictoc::tic(paste("Cleaned file for:", file_year))
      spatial_df_clean <- spatial_df_raw %>%
        rename_with(tolower) %>%
        st_transform(3435) %>%
        st_make_valid() %>%
        filter(!is.na(pin10), !st_is_empty(geometry), st_is_valid(geometry)) %>%
        st_cast("MULTIPOLYGON")
      if (!"pin14" %in% names(spatial_df_clean)) {
        spatial_df_clean <- spatial_df_clean %>%
          select(pin10, geometry) %>%
          mutate(
            pin10 = gsub("\\D", "", pin10),
            pin10 = str_pad(pin10, 10, "left", "0"),
            pin14 = str_pad(pin10, 14, "right", "0")
          )
      } else {
        spatial_df_clean <- spatial_df_clean %>%
          select(pin14, pin10, geometry) %>%
          mutate(
            across(c(pin10, pin14), ~ gsub("\\D", "", .x)),
            pin10 = str_pad(pin10, 10, "left", "0"),
            pin14 = str_pad(pin14, 14, "left", "0"),
            pin10 = ifelse(is.na(pin10), str_sub(pin14, 1, 10), pin10)
          )
      }
      tictoc::toc()

      # Get the centroid of the largest polygon for each parcel
      tictoc::tic(paste("Calculated centroids for:", file_year))
      spatial_df_centroids <- spatial_df_clean %>%
        # Split any multipolygon parcels into multiple rows, one for each
        # polygon
        # https://github.com/r-spatial/sf/issues/763
        st_cast("POLYGON", warn = FALSE) %>%
        # Transform to planar geometry then calculate centroids
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
        summarize(across(c(lon, lat, x_3435, y_3435), dplyr::first)) %>%
        ungroup()
      tictoc::toc()

      # Read attribute data and get unique attributes by PIN10
      tictoc::tic(paste("Merged boundary and attribute data:", file_year))
      attr_df <- read_parquet(local_attr_file) %>%
        mutate(pin10 = str_sub(pin, 1, 10)) %>%
        group_by(pin10) %>%
        summarize(
          across(c(tax_code, nbhd_code, town_code), dplyr::first),
          year = file_year
        ) %>%
        ungroup()

      # Merge spatial boundaries with attribute data and drop sliver polygons
      spatial_df_merged <- spatial_df_clean %>%
        left_join(attr_df, by = "pin10") %>%
        left_join(spatial_df_centroids, by = "pin10") %>%
        mutate(
          has_attributes = !is.na(town_code),
          area = units::drop_units(st_area(geometry))
        ) %>%
        # Drop anything with an area less than 1 sq meter, as long as it has
        # other polygons for the same PIN10 that are larger
        filter(!(area <= 1 & n() >= 2 & !all(area <= 1)), .by = "pin10") %>%
        select(
          pin10, tax_code, nbhd_code, has_attributes,
          lon, lat, x_3435, y_3435, geometry,
          town_code, year
        )
      tictoc::toc()

      # Aggregate PIN10s with more than one row using st_union. Split them out
      # from the rest of the dataset because st_union is an incredibly expensive
      # operation, so we only want to perform it on the PINs we need to
      tictoc::tic(paste("Collapsed parcels to the PIN10 level:", file_year))
      spatial_df_merged <- bind_rows(
        spatial_df_merged %>%
          filter(n() >= 2, .by = pin10) %>%
          summarize(
            across(tax_code:year, dplyr::first),
            across(geometry, st_union),
            .by = "pin10"
          ),
        spatial_df_merged %>%
          filter(!(n() >= 2), .by = pin10)
      ) %>%
        st_cast("MULTIPOLYGON") %>%
        rename(geometry_3435 = geometry) %>%
        mutate(geometry = st_transform(geometry_3435, 4326)) %>%
        relocate(geometry, .after = geometry_3435)
      tictoc::toc()

      # If centroids are missing from join (invalid geom, empty, etc.)
      # fill them in with centroid of the full multipolygon
      tictoc::tic(paste("Repaired centroids:", file_year))
      if (any(
        is.na(spatial_df_merged$lon) |
          any(is.na(spatial_df_merged$x_3435))
      )) {
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
      tictoc::toc()

      tictoc::tic(paste("Calculated shape features:", file_year))
      # Simplify the planar geometry to reduce noise in shape-based features
      # calculated below. Convert to a data.table of the vertices of each
      # polygon. Using data.table here because it's much faster than dplyr
      spatial_mat_coords <- spatial_df_merged %>%
        st_set_geometry("geometry_3435") %>%
        st_simplify(dTolerance = 2, preserveTopology = TRUE) %>%
        st_cast("MULTIPOLYGON") %>%
        st_coordinates() %>%
        as.data.table()

      # Use the coordinate vertices to calculate additional features about
      # the shape of the polygon
      spatial_mat_calc <- spatial_mat_coords[
        ,
        # Calculate the interior angle between each pair of points
        angle := calculate_angles(as.matrix(.SD)),
        .SDcols = c("X", "Y"),
        by = c("L1", "L2", "L3")
      ][
        ,
        # Some vertex angles are close to 180 or 360 degrees, which are likely
        # to be artifacts of the data and not true vertices. We don't want to
        # count these straight angles in our features, BUT we also can't drop
        # all of them since some polygons are basically just lines. So, we drop
        # any straight angle vertices from any polygon with >= 3 non-straight
        # angles (which is the minimum number of vertices needed to make a
        # polygon)
        straight_angle := (
          (
            data.table::between(angle, 0, 1) |
              data.table::between(angle, 359, 360)
          ) | data.table::between(angle, 179, 181)
        )
      ][
        ,
        num_non_straight_angle := sum(!straight_angle, na.rm = TRUE),
        by = c("L1", "L2", "L3")
      ][
        !straight_angle | num_non_straight_angle < 3 | is.na(angle),
      ][
        ,
        `:=`(
          # Get edge length using Pythagorean theorem and a rolling lag to get
          # the distance between each pair of points
          edge_len = sqrt(
            (X - data.table::shift(X, type = "lag"))^2 +
              (Y - data.table::shift(Y, type = "lag"))^2
          ),
          # Distance between the centroid and each point in the polygon
          dist_to_centroid = sqrt(
            (X - mean(X))^2 +
              (Y - mean(Y))^2
          ),
          # Recalculate the angle after removing straight angles
          angle = calculate_angles(as.matrix(.SD))
        ),
        .SDcols = c("X", "Y"),
        by = c("L1", "L2", "L3")
      ]

      # Collapse the vertex-level data back to the parcel level
      spatial_mat_calc_vert <- spatial_mat_calc[
        ,
        .(
          # Number of vertices in the polygon. Minus 1 because the first and
          # last vertex are always identical (used to close the polygon)
          shp_parcel_num_vertices = .N - 1,
          # Tail is used to drop the first row of each group since it's
          # identical to the last row (again, same reason as above)
          shp_parcel_interior_angle_sd = sd(tail(angle, -1), na.rm = TRUE),
          shp_parcel_centroid_dist_ft_sd = sd(tail(dist_to_centroid, -1)),
          shp_parcel_edge_len_ft_sd = sd(tail(edge_len, -1), na.rm = TRUE)
        ),
        by = "L3"
      ]

      # Calculate the ratio of the parcel area to the area of its minimum
      # bounding rectangle
      spatial_df_rec <- spatial_df_merged %>%
        st_set_geometry("geometry") %>%
        st_drop_geometry() %>%
        st_set_geometry("geometry_3435") %>%
        mutate(parcel_area = st_area(geometry_3435)) %>%
        st_minimum_rotated_rectangle() %>%
        mutate(
          shp_parcel_mrr_area_ratio = units::drop_units(
            st_area(geometry_3435) / parcel_area
          ),
          # Sometimes the ratio here is very slightly less than 1, even though
          # that should be impossible. This seems to happen mostly for extremely
          # long, skinny triangle parcels. The error is small enough that it's
          # probably fine to ignore for now
          shp_parcel_mrr_area_ratio = ifelse(
            shp_parcel_mrr_area_ratio < 1,
            1,
            shp_parcel_mrr_area_ratio
          )
        )

      # Calculate the ratio of the sides of the minimum bounding rectangle
      spatial_mat_rec_coords <- spatial_df_rec %>%
        st_coordinates() %>%
        as.data.table()

      spatial_mat_rec_calc <- spatial_mat_rec_coords[
        ,
        edge_len := sqrt(
          (X - data.table::shift(X, type = "lag"))^2 +
            (Y - data.table::shift(Y, type = "lag"))^2
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
      tictoc::toc()

      tictoc::tic(paste("Finalized and wrote to file:", file_year))
      # Merge all calculated features back into the main dataframe
      # then sort by year, town code, and PIN10 for better compression
      spatial_df_final <- spatial_df_merged %>%
        cbind(
          spatial_df_rec %>%
            select(shp_parcel_mrr_area_ratio) %>%
            st_drop_geometry(),
          spatial_mat_rec_calc[, 2],
          spatial_mat_calc_vert[, 2:5]
        ) %>%
        ungroup() %>%
        arrange(year, town_code, pin10) %>%
        relocate(starts_with("shp_"), .after = "y_3435") %>%
        relocate(
          c(shp_parcel_centroid_dist_ft_sd, shp_parcel_interior_angle_sd),
          .after = "shp_parcel_edge_len_ft_sd"
        ) %>%
        # st_make_valid actually uses two different methods, one for planar
        # geometries and a different one for ellipsoidal geometries. The earlier
        # st_make_valid call on the planar geometry (`geometry_3435`) actually
        # still yields one or two parcels that are invalid in the ellipsoidal
        # version, so we call st_make_valid here again just to be safe
        mutate(across(starts_with("geometry"), st_make_valid))

      # Check that the final number of distinct, well-formed parcels is close to
      # the same as the number in the raw parcel file
      num_parcel_raw <- spatial_df_raw %>%
        rename_with(tolower) %>%
        filter(!is.na(pin10), nchar(pin10) == "10", !st_is_empty(geometry)) %>%
        st_drop_geometry() %>%
        summarize(n = n_distinct(pin10)) %>%
        pull(n)
      num_parcel_final <- spatial_df_final %>%
        st_drop_geometry() %>%
        summarize(n = n_distinct(pin10)) %>%
        pull(n)
      if (abs(num_parcel_raw - num_parcel_final) > 10) {
        stop("More than 10 PINs dropped during geometry processing!")
      }

      # Write local backup copy
      geoparquet_to_s3(spatial_df_final, local_backup_file)
      tictoc::toc()
    } else {
      message("Loading processed parcels from backup for: ", file_year)
      spatial_df_final <- read_geoparquet_sf(local_backup_file)
    }

    # Write final dataframe to dataset on S3, partitioned by town and year
    spatial_df_final %>%
      mutate(
        source = "raw",
        uploaded_before_geocoding = TRUE,
        year = file_year
      ) %>%
      relocate(year, town_code, .after = last_col()) %>%
      group_by(year, town_code) %>%
      write_partitions_to_s3(
        s3_bucket_uri,
        is_spatial = TRUE,
        overwrite = FALSE
      )
    tictoc::toc()
  } else {
    message("Skipping already processed data for: ", file_year)
  }
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

gc()

# PARCEL FILLING AND GECODING ----

# Ingest processed parcel files into one dataframe
pre_geocoding_data <- open_dataset(
  file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel_test")
) %>%
  collect() %>%
  # Ensure previously geocoded/imputed parcels don't interfere with geocoding
  # and imputing for any new years of data
  filter(source == "raw") %>%
  mutate(year = as.character(year))

# Get unique combinations of PIN10 and year. There is no reason why the lowest
# value PIN is better, but we do it for a reproducable query.
# We remove info from before 2000 since almost all lon/lat information
# is missing
all_addresses <- dbGetQuery(
  conn = con,
  "
    WITH filtered AS (
      SELECT
        year,
        pin,
        pin10,
        prop_address_full,
        prop_address_city_name,
        prop_address_state,
        prop_address_zipcode_1,
        ROW_NUMBER() OVER (PARTITION BY pin10, year ORDER BY pin ASC) AS rn
      FROM default.vw_pin_address
      WHERE year >= '2000'
        AND prop_address_full IS NOT NULL
        AND prop_address_city_name IS NOT NULL
        AND prop_address_state IS NOT NULL
        AND prop_address_zipcode_1 IS NOT NULL
    )
    SELECT *
    FROM filtered
    WHERE rn = 1
  "
) %>%
  # We can have address data from current year, before parcel data
  # is available.
  filter(year <= max(pre_geocoding_data$year, na.rm = TRUE))

cook_county <- geoarrow::read_geoparquet_sf(
  "s3://ccao-data-warehouse-us-east-1/spatial/ccao/county/2019.parquet"
) %>%
  st_transform(3435)

missing_geographies <- pre_geocoding_data %>%
  full_join(
    all_addresses %>%
      distinct(pin10, year, .keep_all = TRUE),
    by = c("pin10", "year")
  ) %>%
  group_by(pin10, prop_address_full) %>%
  mutate(missing = is.na(x_3435) | is.na(y_3435)) %>%
  arrange(year) %>%
  fill(x_3435, y_3435, lon, lat, .direction = "updown") %>%
  ungroup() %>%
  # Keep only values which in our input had no x-y coordinates
  filter(missing)

# Split the missing parcels into those which were encoded with inputed technique
# and those which still need to be calculated with geocoding
imputed <- missing_geographies %>%
  filter(!is.na(x_3435) & !is.na(y_3435) & !is.na(lon) & !is.na(lat)) %>%
  select(pin10, year, x_3435, y_3435, lon, lat) %>%
  mutate(source = "imputed")

missing_geographies <- missing_geographies %>%
  filter(is.na(x_3435) & is.na(y_3435) & is.na(lon) & is.na(lat)) %>%
  distinct(pin10, year, .keep_all = TRUE) %>%
  select(
    pin10, year, prop_address_full,
    prop_address_city_name, prop_address_state
  )

# Split the missing coordinates into batches of 1000 rows for tidygeocoder
batch_list <- split(
  missing_geographies,
  ceiling(seq_len(nrow(missing_geographies)) / 1000)
)

# Function to geocode each batch
geocode_batch <- function(batch) {
  batch %>%
    geocode(
      street = prop_address_full,
      city   = prop_address_city_name,
      state  = prop_address_state,
      method = "census"
    ) %>%
    filter(!is.na(long) & !is.na(lat)) %>%
    st_as_sf(coords = c("long", "lat"), remove = FALSE) %>%
    st_set_crs(4326) %>%
    st_transform(3435) %>%
    mutate(
      x_3435 = st_coordinates(.)[, 1],
      y_3435 = st_coordinates(.)[, 2]
    )
}

# Apply geocoding to each batch and combine results
geocoded <- map(batch_list, geocode_batch) %>%
  do.call(rbind, .)

geocoded <- geocoded %>%
  # Check that properties are in Cook County Boundaries.
  # This arose due to one known error.
  st_intersection(cook_county) %>%
  # We don't create geographies since these are shapes for other
  # parcels and we only have centroids for these.
  st_drop_geometry() %>%
  select(pin10, year, lat, long, x_3435, y_3435) %>%
  mutate(source = "geocoded")

post_geocoding_data <- bind_rows(pre_geocoding_data, imputed, geocoded)

# Test to make sure we don't duplicate any pins
duplicate_keys <- post_geocoding_data %>%
  group_by(pin10, year) %>%
  filter(n() > 1)

if (nrow(duplicate_keys) > 0) {
  stop("Duplicate rows found pin10 and year combinations. Check rbind")
}

# Write final dataframe to dataset on S3, partitioned by town and year
post_geocoding_data %>%
  mutate(uploaded_before_geocoding = FALSE) %>%
  relocate(year, town_code, .after = last_col()) %>%
  group_by(year, town_code) %>%
  write_partitions_to_s3(
    "s3://ccao-data-warehouse-us-east-1/spatial/parcel_test2",
    is_spatial = TRUE,
    overwrite = TRUE
  )
tictoc::toc()
