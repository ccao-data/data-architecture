library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This scripts cleans and uploads boundaries for various economic incentive
# zones
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "economy")

# Location of file to clean
raw_files <- grep(
  "geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/economy/")$Key
  ),
  value = TRUE
)

# Function to clean consolidated care districts
clean_coordinated_care <- function(shapefile, economic_unit) {
  if (economic_unit == "coordinated_care") {
    shapefile %>%
      mutate(AGENCY_DES = str_replace(AGENCY_DES, "PROVISIO", "PROVISO")) %>%
      group_by(AGENCY_DES, MUNICIPALI) %>%
      summarise() %>%
      mutate(
        cc_num = as.character(NA),
        political_boundary = case_when(
          is.na(MUNICIPALI) ~ "Township",
          TRUE ~ "Municipality"
        ),
        cc_name = str_squish(
          str_to_title(
            case_when(
              is.na(MUNICIPALI) ~
                str_replace(AGENCY_DES, "TWP", ""),
              TRUE ~ MUNICIPALI
            )
          )
        )
      ) %>%
      select(cc_num, cc_name, political_boundary, geometry) %>%
      ungroup()
  } else {
    shapefile
  }
}

# Function to clean enterprise zones
clean_enterprise_zone <- function(shapefile, economic_unit) {
  if (economic_unit == "enterprise_zone") {
    shapefile %>%
      filter(str_detect(County, "Will", negate = TRUE)) %>%
      group_by(Name) %>%
      summarise() %>%
      mutate(ez_num = as.character(NA)) %>%
      select(ez_num, ez_name = Name, geometry) %>%
      ungroup()
  } else {
    shapefile
  }
}

# Function to clean industrial growth zones
clean_industrial_growth_zone <- function(shapefile, economic_unit) {
  if (economic_unit == "industrial_growth_zone") {
    shapefile %>%
      mutate(igz_num = as.character(NA)) %>%
      select(igz_num, igz_name = Name, geometry)
  } else {
    shapefile
  }
}

# Function to clean qualified opportunity zones
clean_qualified_opportunity_zone <- function(shapefile, economic_unit) {
  if (economic_unit == "qualified_opportunity_zone") {
    shapefile %>%
      select(geoid = CENSUSTRAC, geometry)
  } else {
    shapefile
  }
}

# Function to clean the central business district
clean_central_business_district <- function(shapefile, economic_unit) {
  if (economic_unit == "central_business_district") {
    shapefile %>%
      select(cbd_num = objectid, cbd_name = name, geometry)
  } else {
    shapefile
  }
}

# Function to pull raw data from S3 and clean
clean_economy <- function(remote_file) {
  economic_unit <- str_split(remote_file, "/", simplify = TRUE)[1, 6]

  year <- str_split(remote_file, "/", simplify = TRUE)[1, 7] %>%
    gsub(".geojson", "", .)

  tmp_file <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file, file = tmp_file)

  st_read(tmp_file) %>%
    clean_coordinated_care(economic_unit) %>%
    clean_enterprise_zone(economic_unit) %>%
    clean_industrial_growth_zone(economic_unit) %>%
    clean_qualified_opportunity_zone(economic_unit) %>%
    clean_central_business_district(economic_unit) %>%
    standardize_expand_geo(make_valid = TRUE) %>%
    mutate(year = year)

  file.remove(tmp_file)
}

# Apply function to raw_files
cleaned_output <- sapply(raw_files, clean_economy,
  simplify = FALSE, USE.NAMES = TRUE
)
economic_units <- unique(str_split(raw_files, "/", simplify = TRUE)[, 6])

# Function to partition and upload cleaned data to S3
combine_upload <- function(economic_unit) {
  cleaned_output[grep(economic_unit, names(cleaned_output))] %>%
    bind_rows() %>%
    group_by(across(contains("name"))) %>%
    # Some shapefiles don't have consistent identifiers across time,
    # create them using group IDs
    mutate(across(contains("num"), ~ case_when(
      is.na(.) ~ str_pad(cur_group_id(), width = 3, side = "left", pad = "0"),
      TRUE ~ .
    ))) %>%
    group_by(year) %>%
    write_partitions_to_s3(
      file.path(output_bucket, economic_unit),
      is_spatial = TRUE,
      overwrite = TRUE
    )
}

# Apply function to cleaned data
walk(economic_units, combine_upload)
