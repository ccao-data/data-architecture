library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(readr)
library(sf)
library(sfarrow)
library(stringr)
library(tictoc)
library(tidyr)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
school_tmp_dir <- here("school-tmp")

# Get a list of all district-level boundaries
district_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "census", "school")
) %>%
  filter(Size > 0, str_detect(Key, "_district_")) %>%
  mutate(
    year = str_extract(Key, "[0-9]{4}"),
    s3_uri = file.path(AWS_S3_RAW_BUCKET, Key),
    district_type = case_when(
      str_detect(Key, "elementary") ~ "elementary",
      str_detect(Key, "secondary") ~ "secondary",
      str_detect(Key, "unified") ~ "unified",
      TRUE ~ "other"
    )
  ) %>%
  select(year, s3_uri, district_type)


# Geometry file locally for loading with sf
save_local_file <- function(local_path, uri) {
  if (!file.exists(local_path)) {
    print(paste("Grabbing geojson file:", uri))
    aws.s3::save_object(uri, file = local_path)
  }
}


##### COOK DISTRICTS #####

# Clean up and merge non-CPS district files from different years
process_district_file <- function(row) {
  file_year <- row["year"]
  s3_uri <- row["s3_uri"]
  dist_type <- row["district_type"]

  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0("suburban-", dist_type, "-", file_year, ".geojson")
  )
  save_local_file(tmp_file_local, s3_uri)

  df <- sf::read_sf(tmp_file_local) %>%
    select(starts_with(
      c("GEOID", "NAME", "INTPT", "ALAND", "AWATER"),
      ignore.case = TRUE
    )) %>%
    set_names(str_remove_all(names(.), "[[:digit:]]")) %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON")

  df <- df %>%
    st_drop_geometry() %>%
    st_as_sf(coords = c("INTPTLON", "INTPTLAT"), crs = st_crs(df)) %>%
    cbind(st_coordinates(.), st_coordinates(st_transform(., 3435))) %>%
    st_drop_geometry() %>%
    select(lon = X, lat = Y, x_3435 = `X.1`, y_3435 = `Y.1`) %>%
    cbind(df, .) %>%
    select(
      -starts_with("INTPT", ignore.case = TRUE),
      -ends_with("LSAD", ignore.case = TRUE),
      -contains("AWATER", ignore.case = TRUE),
      -contains("ALAND", ignore.case = TRUE)
    ) %>%
    rename_with(tolower) %>%
    mutate(
      year = file_year,
      is_attendance_boundary = FALSE,
      district_type = dist_type,
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      everything(),
      lon, lat, x_3435, y_3435,
      geometry, geometry_3435,
      district_type, year
    )
}

districts_df <- apply(district_files_df, 1, process_district_file) %>%
  bind_rows() %>%
  mutate(across(everything(), unname))


##### CPS #####

# Get a list of all attendance boundaries for CPS
attendance_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "school")
) %>%
  filter(Size > 0, str_detect(Key, "_attendance_")) %>%
  mutate(
    year = str_extract_all(Key, "[0-9]{4}", simplify = TRUE)[,2],
    s3_uri = file.path(AWS_S3_RAW_BUCKET, Key),
    district_type = case_when(
      str_detect(Key, "elementary") ~ "elementary",
      str_detect(Key, "secondary") ~ "secondary",
      TRUE ~ "other"
    )
  ) %>%
  select(year, s3_uri, district_type)


# Clean up and merge CPS boundary files from different years
process_cps_file <- function(row) {
  file_year <- row["year"]
  s3_uri <- row["s3_uri"]
  dist_type <- row["district_type"]

  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0("cps-", dist_type, "-", file_year, ".geojson")
  )
  save_local_file(tmp_file_local, s3_uri)

  df <- st_read(tmp_file_local) %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(centroid = st_centroid(st_transform(geometry, 3435))) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
    ) %>%
    select(-centroid) %>%
    rename_with(~ "school_id", contains(c("school_id", "schoolid"))) %>%
    rename_with(
      ~ "school_nm", contains(
        c("schoolname", "school_nm", "short_name", "school_nam")
      )
    ) %>%
    select(
      geoid = school_id, name = school_nm,
      lon = X, lat = Y, x_3435 = `X.1`, y_3435 = `Y.1`
    ) %>%
    rename_with(tolower) %>%
    mutate(
      name = paste0("CPS ", toupper(dist_type), " - ", name),
      year = file_year,
      is_attendance_boundary = TRUE,
      district_type = dist_type,
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      everything(),
      lon, lat, x_3435, y_3435,
      geometry, geometry_3435,
      district_type, year
    )
}

attendance_df <- apply(
  attendance_files_df %>% filter(as.numeric(year) >= 2011), 1, process_cps_file
) %>%
  bind_rows() %>%
  mutate(across(everything(), unname))

# Merge both datasets and write to S3
bind_rows(
  districts_df,
  attendance_df
) %>%
  group_by(district_type, year) %>%
  group_walk(~ {
    year <- replace_na(.y$year, "__HIVE_DEFAULT_PARTITION__")
    district_type <- replace_na(.y$district_type, "__HIVE_DEFAULT_PARTITION__")
    remote_path <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "spatial", "school", "school_district",
      paste0("district_type=", district_type), paste0("year=", year),
      "part-0.parquet"
    )
    if (!object_exists(remote_path)) {
      print(paste("Now uploading:", year, "data for type:", district_type))
      tmp_file <- tempfile(fileext = ".parquet")
      st_write_parquet(.x, tmp_file, compression = "snappy")
      aws.s3::put_object(tmp_file, remote_path)
    }
  })
