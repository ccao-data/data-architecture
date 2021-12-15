library(aws.s3)
library(dplyr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

# This script retrieves major political boundaries such as townships and
# judicial districts and keeps only necessary columns for reporting and analysis
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Location of file to clean
raw_files <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'spatial/political/')$Key
)

dest_files <- gsub(
  "geojson", "parquet",
  file.path(
    AWS_S3_WAREHOUSE_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = 'spatial/political/')$Key
  )
)

column_names <- c("board_of_review" = "district_n",
                  "commissioner" = "district",
                  "congressional_district" = "district_n",
                  "judicial" = "district",
                  "municipality" = "MUNICIPALITY",
                  "state_representative" = "district_n",
                  "state_senate" = "senatedist",
                  "township" = "NAME",
                  "ward" = "ward")

# Function to pull raw data from S3 and clean
clean_politics <- function(remote_file) {

  political_unit <- str_split(remote_file, "/", simplify = TRUE)[1,6]

  year <- str_split(remote_file, "/", simplify = TRUE)[1,7] %>%
    gsub(".geojson", "", .)

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file, file = tmp_file)

  return(
    st_read(tmp_file) %>%
    mutate_at(vars(contains('MUNICIPALITY')), replace_na, "Unincorporated") %>%
    select(column_names[political_unit], geometry) %>%
    mutate(across(where(is.character), str_to_title),
           geometry_3435 = st_transform(geometry, 3435),
           year = year)
  )

  file.remove(tmp_file)

}

# Apply function to raw_files
cleaned_output <- sapply(raw_files, clean_politics, simplify = FALSE, USE.NAMES = TRUE)

political_units <- unique(str_split(raw_files, "/", simplify = TRUE)[,6])

# Function to parition and upload cleaned data to S3
combine_upload <- function(political_unit) {

  cleaned_output[grep(political_unit, names(cleaned_output))] %>%
    bind_rows() %>%
    group_by(year) %>%
    group_walk(~ {
      year <- replace_na(.y$year, "__HIVE_DEFAULT_PARTITION__")
      remote_path <- file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "political", political_unit,
        paste0("year=", year),
        "part-0.parquet"
      )
      if (!object_exists(remote_path)) {
        print(paste0("Now uploading:", political_unit, "data for ", year))
        tmp_file <- tempfile(fileext = ".parquet")
        st_write_parquet(.x, tmp_file, compression = "snappy")
        aws.s3::put_object(tmp_file, remote_path)
      }
    })

}

# Apply function to cleaned data
lapply(political_units, combine_upload)

 # Cleanup
rm(list = ls())