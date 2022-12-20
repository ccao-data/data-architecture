library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves major political boundaries such as townships and
# judicial districts and keeps only necessary columns for reporting and analysis
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "political")

# Location of file to clean
raw_files <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/political/")$Key
)
dest_files <- gsub(
  "geojson", "parquet",
  file.path(
    AWS_S3_WAREHOUSE_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/political/")$Key
  )
)
column_names_old <- c(
  "board_of_review_district" = "district_n",
  "commissioner_district" = "district",
  "congressional_district" = "district_n",
  "judicial_district" = "district",
  "municipality" = "MUNICIPALITY",
  "state_representative_district" = "district_n",
  "state_senate_district" = "senatedist",
  "ward_chicago" = "ward",
  "ward_evanston" = "ward"
)

column_names_2022 <- c(
  "judicial_district" = "NAME2"
)

column_names_2023 <- c(
  "board_of_review_district" = "DISTRICT_TXT",
  "commissioner_district" = "DISTRICT_TXT",
  "congressional_district" = "DISTRICT_TXT",
  "state_representative_district" = "DISTRICT_TXT",
  "state_senate_district" = "DISTRICT_TXT"
)

# Function to pull raw data from S3 and clean
clean_politics <- function(remote_file) {

  political_unit <- str_split(remote_file, "/", simplify = TRUE)[1, 6]

  print(political_unit)

  year <- str_split(remote_file, "/", simplify = TRUE)[1, 7] %>%
    gsub(".geojson", "", .)

  print(year)

  column_names <- if (year == "2023" &
                      political_unit %in% names(column_names_2023)) {

    column_names_2023

  } else if (year == "2022" &
       political_unit %in% names(column_names_2022)) {

    column_names_2022

  } else column_names_old

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file, file = tmp_file)

  return(
    temp <- st_read(tmp_file) %>%
      mutate_at(vars(contains("MUNICIPALITY")), replace_na, "Unincorporated") %>%
      select(column_names[political_unit], any_of("AGENCY"), geometry) %>%
      mutate(
        across(where(is.character), ~ str_replace(.x, "//.0", "")),
        across(where(is.character), str_to_title),
        across(where(is.character), readr::parse_number, .names = "{.col}_num"),
        geometry_3435 = st_transform(geometry, 3435),
        year = year
      ) %>%
      rename_with(~ paste0(.x, "_name"), names(column_names[political_unit])) %>%
      mutate(across(ends_with("_name"), as.character)) %>%
      select(-any_of("municipality_num")) %>%
      rename_with(~ "municipality_num", any_of("AGENCY")) %>%
      select(ends_with("_num"), everything(), geometry, geometry_3435, year) %>%
      na.omit()
  )

  file.remove(tmp_file)
}

# Apply function to raw_files
cleaned_output <- sapply(raw_files, clean_politics, simplify = FALSE, USE.NAMES = TRUE)

political_units <- unique(str_split(raw_files, "/", simplify = TRUE)[, 6])

# Function to partition and upload cleaned data to S3
combine_upload <- function(political_unit) {
  cleaned_output[grep(political_unit, names(cleaned_output))] %>%
    bind_rows() %>%
    group_by(year) %>%
    write_partitions_to_s3(
      file.path(output_bucket, political_unit),
      is_spatial = TRUE,
      overwrite = FALSE
    )
}

# Apply function to cleaned data
walk(political_units, combine_upload)
