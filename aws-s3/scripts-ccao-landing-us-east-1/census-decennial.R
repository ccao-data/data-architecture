library(arrow)
library(aws.s3)
library(dplyr)
library(stringr)
library(tidycensus)

# This script retrieves raw ACS data for the data lake
# It populates the raw s3 bucket
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# Retrieve census API key from local .Renviron
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# Declare years we'd like to grab census data for
census_years <- Sys.getenv("CENSUS_DEC_MIN_YEAR"):Sys.getenv("CENSUS_DEC_MAX_YEAR")

# All decennial census variables we're looking to grab
census_variables <- c(
  "tot_pop"   = "P003001", # Total population
  "white"     = "P003002", # White alone
  "black"     = "P003003", # Black or African American alone
  "am_ind"    = "P003004", # American Indian and Alaska Native alone
  "asian"     = "P003005", # Asian alone
  "pac_isl"   = "P003006", # Native Hawaiian and Other Pacific Islander alone
  "o1"        = "P003007", # Some other race alone
  "o2"        = "P003008", # Two or more races
  "his"       = "P004001"  # Hispanic or Latino
)

# Declare geographies we'd like to query
census_geographies <- c(
  "block",
  "block group",
  "county",
  "county subdivision",
  "school district (elementary)",
  "school district (secondary)",
  "school district (unified)",
  "state legislative district (lower chamber)",
  "state legislative district (upper chamber)",
  "tract",
  "zcta"
)

# Folders have a different naming schema than geographies
folders <- c(
  "block",
  "block_group",
  "county",
  "county_subdivision",
  "school_district_elementary",
  "school_district_secondary",
  "school_district_unified",
  "state_representative",
  "state_senate",
  "tract",
  "zcta"
)

# Link geographies and folders
folders_df <- data.frame(geography = census_geographies, folder = folders)

# Generate a combination of all years, geographies, and tables
all_combos <- expand.grid(
  geography = census_geographies,
  year = census_years,
  survey = "decennial",
  stringsAsFactors = FALSE
) |>
  # Join on folder names
  left_join(folders_df)

# Function to loop through rows in all_combos, grab census data,
# and write it to a parquet file on S3 if it doesn't already exist
pull_and_write_acs <- function(x) {
  survey <- x["survey"]
  folder <- x["folder"]
  geography <- x["geography"]
  year <- x["year"]

  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "census", survey, folder, paste0(year, ".parquet")
  )

  # Check to see if file already exists on S3; if it does, skip it
  if (!aws.s3::object_exists(remote_file)) {

    # Print file being written
    print(paste0(Sys.time(), " - ", remote_file))

    # These geographies are county-specific rather than state-level
    county_specific <- c("county", "county subdivision", "tract")
    county <- if (geography %in% county_specific) "Cook" else NULL

    # Retrieve specified data from census API
    get_decennial(
      geography = geography,
      variables = census_variables,
      output = "wide",
      state = "IL",
      county = county,
      year = as.numeric(year),
      cache_table = TRUE
    ) |>
      # Clean output, write to parquet files
      dplyr::rename("geoid" = "GEOID", "geography" = "NAME") |>
      arrow::write_parquet(remote_file)
  }
}

# Apply function to all_combos
apply(all_combos, 1, pull_and_write_acs)

# Cleanup
rm(list = ls())