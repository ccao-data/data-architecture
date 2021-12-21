library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(readr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "school")
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


##### COOK CENSUS DISTRICTS #####

# Clean up and merge non-CPS district files from different years
process_district_file <- function(s3_bucket_uri, file_year, uri, dist_type) {

  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0("suburban-", dist_type, "-", file_year, ".geojson")
  )
  save_s3_to_local(uri, tmp_file_local)

  df <- sf::read_sf(tmp_file_local) %>%
    select(starts_with(
      c("GEOID", "NAME", "INTPT", "ALAND", "AWATER"),
      ignore.case = TRUE
    )) %>%
    set_names(str_remove_all(names(.), "[[:digit:]]")) %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON")

  df %>%
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
    filter(GEOID != '1729100') %>% # Oak Grove School District should only have one polygon, in NE IL
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

# Apply function to all district files and merge output
districts_df <- pmap_dfr(district_files_df, function(...) {
  df <- tibble::tibble(...)
  process_district_file(
    s3_bucket_uri = output_bucket,
    file_year = df$year,
    uri = df$s3_uri,
    dist_type = df$district_type
  )
}) %>%
  mutate(across(everything(), unname))


##### CPS #####

# Get a list of all attendance boundaries for CPS
attendance_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "school")
) %>%
  filter(Size > 0, str_detect(Key, "_attendance_")) %>%
  mutate(
    year = str_extract_all(Key, "[0-9]{4}", simplify = TRUE)[, 2],
    s3_uri = file.path(AWS_S3_RAW_BUCKET, Key),
    district_type = case_when(
      str_detect(Key, "elementary") ~ "elementary",
      str_detect(Key, "secondary") ~ "secondary",
      TRUE ~ "other"
    )
  ) %>%
  select(year, s3_uri, district_type)


# Clean up and merge CPS boundary files from different years
process_cps_file <- function(s3_bucket_uri, file_year, uri, dist_type) {

  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0("cps-", dist_type, "-", file_year, ".geojson")
  )
  save_s3_to_local(uri, tmp_file_local)

  st_read(tmp_file_local) %>%
    rename_with(~"school_id", contains(c("school_id", "schoolid"))) %>%
    rename_with(
      ~"school_nm", contains(
        c("schoolname", "school_nm", "short_name", "school_nam")
      )
    ) %>%
    # GAGE and ENGLEWOOD secondary attendance boundaries overlapped in 2020/2021
    # As GAGE was reduced in size due to school closures and a new STEM school in ENGLEWOOD
    # The new district boundaries are what we're interested in
    filter(!(school_nm == 'GAGE PARK HS' & boundarygr != '9, 10, 11, 12')) %>%
    mutate(school_nm = str_replace(school_nm, "H S", "HS")) %>%
    group_by(grade_cat, school_id, school_nm) %>%
    summarise() %>%
    ungroup() %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(centroid = st_centroid(st_transform(geometry, 3435))) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
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

attendance_df <- pmap_dfr(
  attendance_files_df %>%
    filter(as.numeric(year) >= 2011),
  function(...) {
    df <- tibble::tibble(...)
    process_cps_file(
      s3_bucket_uri = output_bucket,
      file_year = df$year,
      uri = df$s3_uri,
      dist_type = df$district_type
    )
  }
) %>%
  mutate(across(everything(), unname))

# Merge both datasets and write to S3
bind_rows(
  districts_df,
  attendance_df
) %>%
  group_by(district_type, year) %>%
  write_partitions_to_s3(
    file.path(output_bucket, "school_district"),
    is_spatial = TRUE,
    overwrite = TRUE
  )



##### SCHOOL LOCATIONS #####
location_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "school", "location")
) %>%
  mutate(
    year = str_extract(Key, "[0-9]{4}"),
    s3_uri = file.path(AWS_S3_RAW_BUCKET, Key)
  )

# Clean up school locations for each file
process_location_file <- function(file_year, uri) {

  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0("location-", file_year, ".geojson")
  )
  save_s3_to_local(uri, tmp_file_local)

  sf::read_sf(tmp_file_local) %>%
    rename_with(tolower) %>%
    mutate(year = file_year) %>%
    select(
      name = cfname, type = cfsubtype, address, gniscode, comment,
      jurisdiction, community, year
    ) %>%
    st_transform(4326) %>%
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
    st_cast("POINT")
}

# Apply function to all location files and write to S3
pmap_dfr(location_files_df, function(...) {
  df <- tibble::tibble(...)
  process_location_file(
    file_year = df$year,
    uri = df$s3_uri
  )
}) %>%
  group_by(year) %>%
  write_partitions_to_s3(
    file.path(output_bucket, "school_location"),
    is_spatial = TRUE,
    overwrite = TRUE
  )