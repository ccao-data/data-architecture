library(aws.s3)
library(dplyr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)

# This script cleans tax boundaries and uploads them to the S3 warehouse.
# Users must link tow raw column names to pre-selected column names for each shapefile
# That will be cleaned and uplaoded since there isn't consistent across taxing bodies and time.
# Those tow columns are agency number and the name of the tax body - geometry doesn't need to be linked.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Location of file to clean
raw_files <- grep(
  "geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/tax/")$Key
  ),
  value = TRUE
)

# Link raw column names to standardized column names
column_names <- list(
  "community_college/2012" = c("community_college_num" = "agency", "community_college_name" = "max_agency", "geometry"),
  "community_college/2013" = c("community_college_num" = "agency", "community_college_name" = "max_agency", "geometry"),
  "community_college/2014" = c("community_college_num" = "agency", "community_college_name" = "max_agency", "geometry"),
  "community_college/2015" = c("community_college_num" = "agency", "community_college_name" = "max_agency", "geometry"),
  "community_college/2016" = c("community_college_num" = "agency", "community_college_name" = "max_agency", "geometry"),
  "fire_protection/2015" = c("fire_protection_num" = "agency", "fire_protection_name" = "agency_des", "geometry"),
  "fire_protection/2016" = c("fire_protection_num" = "agency", "fire_protection_name" = "agency_des", "geometry"),
  "fire_protection/2018" = c("fire_protection_num" = "AGENCY", "fire_protection_name" = "AGENCY_DESCRIPTION", "geometry"),
  "library/2015" = c("library_num" = "agency", "library_name" = "max_agency", "geometry"),
  "library/2016" = c("library_num" = "agency", "library_name" = "max_agency", "geometry"),
  "library/2018" = c("library_num" = "AGENCY", "library_name" = "MAX_AGENCY_DESC", "geometry"),
  "park/2015" = c("park_num" = "agency", "park_name" = "agency_des", "geometry"),
  "park/2016" = c("park_num" = "agency", "park_name" = "agency_des", "geometry"),
  "park/2018" = c("park_num" = "AGENCY", "park_name" = "AGENCY_DESCRIPTION", "geometry"),
  "sanitation/2018" = c("ssa_num" = "AGENCY", "sanitation_name" = "MAX_AGENCY_DESC", "geometry"),
  "ssa/2020" = c("tif_num" = "AGENCY", "ssa_name" = "AGENCY_DES", "geometry"),
  "tif/2015" = c("tif_num" = "agencynum", "tif_name" = "tif_name", "geometry"),
  "tif/2016" = c("tif_num" = "agencynum", "tif_name" = "agency_des", "geometry"),
  "tif/2018" = c("tif_num" = "AGENCYNUM", "tif_name" = "AGENCY_DESCRIPTION", "geometry")
)

# Function to pull raw data from S3 and clean
clean_tax <- function(remote_file) {
  tax_body <- str_split(remote_file, "/", simplify = TRUE)[1, 6]

  year <- str_split(remote_file, "/", simplify = TRUE)[1, 7] %>%
    gsub(".geojson", "", .)

  tax_body_year <- paste0(tax_body, "/", year)

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file, file = tmp_file)

  return(
    st_read(tmp_file) %>%
      select(column_names[[tax_body_year]]) %>%
      mutate(across(contains("num"), as.character),
        across(where(is.character), str_to_title),
        geometry_3435 = st_transform(geometry, 3435),
        year = year
      )
  )

  file.remove(tmp_file)
}

# Apply function to raw_files
cleaned_output <- sapply(raw_files, clean_tax, simplify = FALSE, USE.NAMES = TRUE)

tax_bodies <- unique(str_split(raw_files, "/", simplify = TRUE)[, 6])

# Function to parition and upload cleaned data to S3
combine_upload <- function(tax_body) {
  cleaned_output[grep(tax_body, names(cleaned_output))] %>%
    bind_rows() %>%
    group_by(year) %>%
    group_walk(~ {
      year <- replace_na(.y$year, "__HIVE_DEFAULT_PARTITION__")
      remote_path <- file.path(
        AWS_S3_WAREHOUSE_BUCKET, "spatial", "tax", tax_body,
        paste0("year=", year),
        "part-0.parquet"
      )
      if (!object_exists(remote_path)) {
        print(paste0("Now uploading:", tax_body, "data for ", year))
        tmp_file <- tempfile(fileext = ".parquet")
        st_write_parquet(.x, tmp_file, compression = "snappy")
        aws.s3::put_object(tmp_file, remote_path)
      }
    })
}

# Apply function to cleaned data
lapply(tax_bodies, combine_upload)

# Cleanup
rm(list = ls())
