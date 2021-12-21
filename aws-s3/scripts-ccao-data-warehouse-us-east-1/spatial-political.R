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
column_names <- c(
  "board_of_review_district" = "district_n",
  "commissioner_district" = "district",
  "congressional_district" = "district_n",
  "judicial_district" = "district",
  "municipality" = "MUNICIPALITY",
  "state_representative_district" = "district_n",
  "state_senate_district" = "senatedist",
  "ward" = "ward"
)

# Function to pull raw data from S3 and clean
clean_politics <- function(remote_file) {
  political_unit <- str_split(remote_file, "/", simplify = TRUE)[1, 6]

  year <- str_split(remote_file, "/", simplify = TRUE)[1, 7] %>%
    gsub(".geojson", "", .)

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file, file = tmp_file)

  return(
    st_read(tmp_file) %>%
      mutate_at(vars(contains("MUNICIPALITY")), replace_na, "Unincorporated") %>%
      select(column_names[political_unit], geometry) %>%
      mutate(
        across(where(is.character), str_to_title),
        across(where(is.character), readr::parse_number, .names = "{.col}_num"),
        geometry_3435 = st_transform(geometry, 3435),
        year = year
      ) %>%
      rename_with(~ paste0(.x, "_name"), names(column_names[political_unit])) %>%
      select(-any_of("municipality_num")) %>%
      select(ends_with("_num"), everything(), geometry, geometry_3435, year)
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
      overwrite = TRUE
    )
}

# Apply function to cleaned data
walk(political_units, combine_upload)
