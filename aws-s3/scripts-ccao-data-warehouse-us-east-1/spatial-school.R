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
    ),
    district = "suburban"
  ) %>%
  select(year, s3_uri, district, district_type)

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
    ),
    district = "cps"
  ) %>%
  select(year, s3_uri, district, district_type)



# Save S3 parcel and attribute files locally for loading with sf
save_local_file <- function(local_path, uri) {
  if (!file.exists(local_path)) {
    print(paste("Grabbing geojson file:", uri))
    aws.s3::save_object(uri, file = local_path)
  }
}


process_district_file <- function(row) {
  file_year <- row["year"]
  s3_uri <- row["s3_uri"]
  district <- row["district"]
  dist_type <- row["district_type"]

  # Download S3 files to local temp dir if they don't exist
  tmp_file_local <- file.path(
    school_tmp_dir,
    paste0(district, "-", dist_type, "-", file_year, ".geojson")
  )
  save_local_file(tmp_file_local, s3_uri)

  spatial_df <- st_read(tmp_file_local) %>%
    rename_with(tolower) %>%
    rename_with(~ "agency_des", starts_with(c("agency_des", "max_agency"))) %>%
    mutate(
      agency_desc = str_trim(str_squish(agency_des)),
      agency_desc = gsub("[^[:alnum:][:space:]/]","", agency_desc),
      is_cps = district == "cps",
      district_type = dist_type,
      year = file_year,
      agency = as.character(agency)
    ) %>%
    select(
      agency, agency_desc,
      is_cps, geometry, district_type, year
    )
}

# Clean up and merge non-CPS district files from different years
districts_df <- apply(district_files_df, 1, process_district_file) %>%
  bind_rows() %>%
  mutate(
    agency_id = ifelse(as.numeric(year) >= 2018, NA, agency),
    agency_tax_num = ifelse(as.numeric(year) >= 2018, agency, NA)
  ) %>%
  filter(!is.na(agency_desc), agency_desc != "") %>%
  st_make_valid() %>%
  mutate(area = st_area(geometry)) %>%
  group_by(agency_desc, year) %>%
  arrange(year, desc(area)) %>%
  summarise(
    across(
      all_of(c("agency_id", "agency_tax_num", "district_type", "is_cps")),
      first
    ),
    geometry = st_union(geometry)
  ) %>%
  group_by(agency_desc) %>%
  fill(agency_tax_num, .direction = "up") %>%
  fill(agency_id, .direction = "down")
