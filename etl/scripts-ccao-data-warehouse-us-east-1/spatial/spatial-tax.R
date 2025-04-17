library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans tax boundaries and uploads them to the S3 warehouse.
# Users must link tow raw column names to pre-selected column names for each
# shapefile that will be cleaned and uplaoded since there isn't consistent
# across taxing bodies and time. Those tow columns are agency number and the
# name of the tax body - geometry doesn't need to be linked.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "tax")

# Tax districts to clean
districts <- c(
  "community_college_district",
  "fire_protection_district",
  "library_district",
  "park_district",
  "sanitation_district",
  "special_service_area",
  "tif_district"
)

raw_files <- lapply(districts, function(x) {
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = file.path("spatial/tax", x)
    )$Key
  )
}) %>%
  unlist() %>%
  sapply(function(x) {
    tmp_file <- tempfile(fileext = ".geojson")
    aws.s3::save_object(x, file = tmp_file)

    st_read(tmp_file)
  }, simplify = TRUE, USE.NAMES = TRUE)


# Function to clean shapefiles
clean_files <- sapply(names(raw_files), function(x) {
  message(paste0("processing "), x)

  new_cols <- c(paste0(basename(dirname(x)), c("_num", "_name")))

  raw_files[[x]] %>%
    st_make_valid() %>%
    st_transform(4326) %>%
    rename_all(tolower) %>%
    select(contains("agency") & !contains("num")) %>%
    rename_with(.cols = contains("agency"), ~new_cols) %>%
    drop_na() %>%
    filter_at(vars(new_cols[2]), all_vars(str_squish(.) != "")) %>%
    group_by_at(new_cols[1:2]) %>%
    summarise() %>%
    ungroup() %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(
      across(ends_with("num"), as.numeric),
      geometry_3435 = st_transform(geometry, 3435),
      year = str_extract(x, "[0-9]{4}")
    )
}, simplify = FALSE, USE.NAMES = TRUE)

# Upload to S3 by district
districts %>%
  walk(function(x) {
    message(x)

    bind_rows(clean_files[grepl(x, names(clean_files))]) %>%
      group_by_at(vars(starts_with(x))) %>%
      mutate(across(ends_with("num"), ~ min(.x))) %>%
      group_by(year) %>%
      # Upload to s3
      write_partitions_to_s3(
        file.path(output_bucket, x),
        is_spatial = TRUE,
        overwrite = TRUE
      )
  })

# Sidwell grid: Originally formatted as Cook County Clerk tax map pages which
# are Sidwell grid "sections" divided into eighths. We dissolve these pages back
# into sections. Sections are 1-mile square subdivisions of "areas" (townships,
# but with sidwell's numbering system rather than the county's) and are
# identified by the first four digits of any PIN.
tmp_file <- tempfile(fileext = ".geojson")
aws.s3::save_object(
  file.path(AWS_S3_RAW_BUCKET, "spatial/tax/sidwell_grid/sidwell_grid.geojson"),
  file = tmp_file
)
st_read(tmp_file) %>%
  # Make sure to perform union on projected rather than geographic coordinate
  # system
  st_transform(crs = 3435) %>%
  mutate(section = substr(PAGE, 1, 4)) %>%
  group_by(section) %>%
  summarise() %>%
  # Buffering the polygons very slightly removes a lot of odd artifacts from
  # within them
  st_buffer(10) %>%
  st_buffer(-10) %>%
  st_transform(crs = 4326) %>%
  mutate(
    geometry_3435 = st_transform(geometry, 3435),
    year = "2025"
  ) %>%
  geoparquet_to_s3(
    file.path(output_bucket, "sidwell_grid", "sidwell_grid.parquet")
    )
