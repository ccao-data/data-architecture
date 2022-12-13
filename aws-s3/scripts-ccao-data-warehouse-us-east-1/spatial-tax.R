library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans tax boundaries and uploads them to the S3 warehouse.
# Users must link tow raw column names to pre-selected column names for each shapefile
# That will be cleaned and uplaoded since there isn't consistent across taxing bodies and time.
# Those tow columns are agency number and the name of the tax body - geometry doesn't need to be linked.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "tax")

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
  "community_college_district/2012" = c("community_college_district_num" = "agency", "community_college_district_name" = "max_agency", "geometry"),
  "community_college_district/2013" = c("community_college_district_num" = "agency", "community_college_district_name" = "max_agency", "geometry"),
  "community_college_district/2014" = c("community_college_district_num" = "agency", "community_college_district_name" = "max_agency", "geometry"),
  "community_college_district/2015" = c("community_college_district_num" = "agency", "community_college_district_name" = "max_agency", "geometry"),
  "community_college_district/2016" = c("community_college_district_num" = "agency", "community_college_district_name" = "max_agency", "geometry"),
  "fire_protection_district/2015" = c("fire_protection_district_num" = "agency", "fire_protection_district_name" = "agency_des", "geometry"),
  "fire_protection_district/2016" = c("fire_protection_district_num" = "agency", "fire_protection_district_name" = "agency_des", "geometry"),
  "fire_protection_district/2018" = c("fire_protection_district_num" = "AGENCY", "fire_protection_district_name" = "AGENCY_DESCRIPTION", "geometry"),
  "library_district/2015" = c("library_district_num" = "agency", "library_district_name" = "max_agency", "geometry"),
  "library_district/2016" = c("library_district_num" = "agency", "library_district_name" = "max_agency", "geometry"),
  "library_district/2018" = c("library_district_num" = "AGENCY", "library_district_name" = "MAX_AGENCY_DESC", "geometry"),
  "park_district/2015" = c("park_district_num" = "agency", "park_district_name" = "agency_des", "geometry"),
  "park_district/2016" = c("park_district_num" = "agency", "park_district_name" = "agency_des", "geometry"),
  "park_district/2018" = c("park_district_num" = "AGENCY", "park_district_name" = "AGENCY_DESCRIPTION", "geometry"),
  "sanitation_district/2018" = c("sanitation_district_num" = "AGENCY", "sanitation_district_name" = "MAX_AGENCY_DESC", "geometry"),
  "special_service_area/2020" = c("special_service_area_num" = "AGENCY", "special_service_area_name" = "AGENCY_DES", "geometry"),
  "tif_district/2015" = c("tif_district_num" = "agencynum", "tif_district_name" = "tif_name", "geometry"),
  "tif_district/2016" = c("tif_district_num" = "agencynum", "tif_district_name" = "agency_des", "geometry"),
  "tif_district/2018" = c("tif_district_num" = "AGENCYNUM", "tif_district_name" = "AGENCY_DESCRIPTION", "geometry")
)

# Function to clean fire protection districts
clean_fire_protection <- function(shapefile, tax_body) {

  if (tax_body == "fire_protection") {

    return(

      shapefile %>%
        mutate(
          fire_protection_name = gsub(
            pattern = "Fire Dist|Fire Prot Dist|Fire Protection Dist",
            replacement = "Fire Protection District",
            fire_protection_name,
            ignore.case = TRUE)) %>%
        mutate(fire_protection_name = gsub(
          pattern = "Districtrict",
          replacement = "District",
          fire_protection_name,
          ignore.case = TRUE)) %>%
        mutate(fire_protection_name = case_when(fire_protection_num == "6240" ~ "NORTHBROOK RURAL Fire Protection District",
                                                TRUE ~ fire_protection_name),
               fire_protection_num = NA)

    )

  } else {

    return(shapefile)

  }

}

# Function to clean library and park districts
clean_general <- function(shapefile, tax_body) {

  if (tax_body %in% c("library", "park")) {

    return(

      shapefile %>%
        mutate(across(contains("num"), ~ NA)) %>%
        mutate(across(contains("name"), ~ gsub("Districtrict", "District",
                                               gsub("Dist", "District", ., ignore.case = TRUE),
                                               ignore.case = TRUE)))

    )

  } else {

    return(shapefile)

  }

}

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
      filter(across(contains("name"), ~ !is.na(.x))) %>%

      # Apply specific cleaning functions
      clean_fire_protection(tax_body = tax_body) %>%
      clean_general(tax_body = tax_body) %>%

      # Some shapefiles have multiple rows per tax body rather than multipolygons, fix that
      st_transform(4326) %>%
      st_make_valid() %>%
      group_by(across(contains(c("num", "name")))) %>%
      summarise() %>%
      ungroup() %>%

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

# Function to partition and upload cleaned data to S3
combine_upload <- function(tax_body) {
  cleaned_output[grep(tax_body, names(cleaned_output))] %>%
    bind_rows() %>%
    group_by(across(contains("name"))) %>%

    # Some shapefiles don't have consistent identifiers across time, create them using group IDs
    mutate(across(contains("num"), ~ case_when(
      is.na(.x) ~ str_pad(cur_group_id(), width = 3, side = "left", pad = "0"),
                                               TRUE ~ .x
      ))) %>%
    group_by(year) %>%
    write_partitions_to_s3(
      file.path(output_bucket, tax_body),
      is_spatial = TRUE,
      overwrite = TRUE
    )
}

# Apply function to cleaned data
walk(tax_bodies, combine_upload)