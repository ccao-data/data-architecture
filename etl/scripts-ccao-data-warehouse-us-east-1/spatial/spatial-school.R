library(arrow)
library(aws.s3)
library(dplyr)
library(here)
library(purrr)
library(readr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "school")
school_tmp_dir <- here("school-tmp")

##### COOK CENSUS DISTRICTS #####

# Get a list of all district-level boundaries
census_district_files_df <- aws.s3::get_bucket_df(
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

# Clean up and merge non-CPS district files from different years
process_census_district_file <- function(
    s3_bucket_uri, file_year, uri, dist_type) {
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
    # Oak Grove School District should only haveone polygon, in NE IL
    filter(GEOID != "1729100") %>%
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
census_districts_df <- pmap_dfr(census_district_files_df, function(...) {
  df <- tibble::tibble(...)
  process_census_district_file(
    s3_bucket_uri = output_bucket,
    file_year = df$year,
    uri = df$s3_uri,
    dist_type = df$district_type
  )
}) %>%
  mutate(across(everything(), unname))

unlink(file.path(school_tmp_dir, "*"))

##### COOK CLERK DISTRICTS #####

# Get a list of all district-level boundaries
county_district_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "school")
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


# Clean up and merge non-CPS district files from different years
process_county_district_file <- function(
    s3_bucket_uri, file_year, uri, dist_type) {
  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0("suburban-", dist_type, "-", file_year, ".geojson")
  )
  if (!file.exists(tmp_file_local)) save_s3_to_local(uri, tmp_file_local)

  df <- sf::read_sf(tmp_file_local) %>%
    st_transform(4326) %>%
    st_make_valid() %>%
    select(contains("DESC"), geometry) %>%
    rename_with(~"school_nm", contains("DESC", ignore.case = TRUE)) %>%
    mutate(
      school_nm = str_replace(school_nm, "C C", ""),
      school_nm = str_replace(
        school_nm,
        "LINCOLNWAY HIGH SCHOOL 210",
        "RICH TOWNSHIP HIGH SCHOOL 227"
      )
    ) %>%
    mutate(school_nm = str_squish(school_nm)) %>%
    filter(str_detect(school_nm, "[:alpha:]")) %>%
    mutate(
      school_num = str_squish(str_remove_all(school_nm, "[[:alpha:].#]")),
      year = file_year,
      district_type = dist_type
    ) %>%
    filter(str_detect(school_num, "[:digit:]")) %>%
    filter(!st_is_empty(.)) %>%
    select(contains(c("school", "district_type", "year", "geometry"))) %>%
    group_by(school_nm, school_num, district_type, year) %>%
    summarise() %>%
    ungroup() %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435),
      centroid = st_centroid(st_transform(geometry, 3435)),
      is_attendance_boundary = FALSE
    ) %>%
    cbind(
      st_coordinates(st_transform(.$centroid, 4326)),
      st_coordinates(.$centroid)
    ) %>%
    select(
      name = school_nm, school_num,
      lon = X, lat = Y, x_3435 = `X.1`, y_3435 = `Y.1`,
      year, is_attendance_boundary, district_type, geometry, geometry_3435
    )
}

# Apply function to all district files and merge output
county_districts_df <- pmap_dfr(county_district_files_df, function(...) {
  df <- tibble::tibble(...)
  process_county_district_file(
    s3_bucket_uri = output_bucket,
    file_year = df$year,
    uri = df$s3_uri,
    dist_type = df$district_type
  )
}) %>%
  mutate(across(everything(), unname))

unlink(file.path(school_tmp_dir, "*"))

# Add geoids to county districts
county_districts_df <- st_join(
  county_districts_df %>%
    filter(year %in% unique(census_districts_df$year)) %>%
    mutate(
      border = geometry,
      year = as.character(as.numeric(year) + 1)
    ) %>%
    st_centroid(),
  census_districts_df %>%
    select(geoid, census_year = year, census_district_type = district_type)
) %>%
  group_by(school_num, district_type, year) %>%
  mutate(
    matches = sum(year == census_year & district_type == census_district_type)
  ) %>%
  mutate(geoid = case_when(
    matches == 0 ~ NA,
    TRUE ~ geoid
  )) %>%
  ungroup() %>%
  filter(
    (year == census_year & district_type == census_district_type) | is.na(geoid)
  ) %>%
  mutate(geoid = case_when(
    district_type == "unified" & school_num == "205" ~ "1713970",
    district_type == "elementary" & school_num == "100" ~ "1706090",
    district_type == "elementary" & school_num == "125" ~ "1704560",
    school_num == "180" ~ "1730510",
    school_num == "157-" ~ "1715700",
    school_num == "110" ~ "1737860",
    TRUE ~ geoid
  )) %>%
  select(-contains("census")) %>%
  mutate(geometry = border) %>%
  select(-c(border, matches)) %>%
  distinct() %>%
  bind_rows(
    county_districts_df %>%
      filter(!(year %in% unique(census_districts_df$year)))
  ) %>%
  select(geoid, everything())

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
    st_make_valid() %>%
    st_transform(4326) %>%
    rename_with(~"school_id", contains(c("school_id", "schoolid"))) %>%
    rename_with(
      ~"school_nm", contains(
        c("schoolname", "school_nm", "short_name", "school_nam")
      )
    ) %>%
    # GAGE/BOGAN/PHILLIPS/CHICAGO VOCATIONAL and ENGLEWOOD secondary attendance
    # boundaries overlapped in 2020/2021. As
    # GAGE/BOGAN/PHILLIPS/CHICAGO VOCATIONAL were reduced in size due to school
    # closures and a new STEM school in ENGLEWOOD the new district boundaries
    # are what we're interested in. We need to be specific about which grades
    # and years we exclude or some schools in 2017/2018 that should be ingested
    # will not be.
    filter(!(
      str_detect(school_nm, "GAGE|BOGAN|PHILLIPS|CHICAGO VOCATIONAL") &
        boundarygr != "9, 10, 11, 12" &
        !(file_year %in% c("2017", "2018"))
    )) %>%
    mutate(school_nm = str_replace(school_nm, "H S", "HS")) %>%
    group_by(grade_cat, school_id, school_nm) %>%
    summarise() %>%
    ungroup() %>%
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
  county_districts_df,
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
