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
census_years <- Sys.getenv("CENSUS_ACS_MIN_YEAR"):Sys.getenv("CENSUS_ACS_MAX_YEAR")

# All ACS variables we're looking to grab
census_variables <- c(
  "tot_pop"   = "B03002_001", # Total population
  "white"     = "B03002_003", # White alone
  "black"     = "B03002_004", # Black or African American alone
  "am_ind"    = "B03002_005", # American Indian and Alaska Native alone
  "asian"     = "B03002_006", # Asian alone
  "pac_isl"   = "B03002_007", # Native Hawaiian and Other Pacific Islander alone
  "o1"        = "B03002_008", # Some other race alone
  "o2"        = "B03002_009", # Two or more races
  "o3"        = "B03002_010", # Two races including other race
  "o4"        = "B03002_011", # Two races excluding other race, and 3+ races
  "his"       = "B03002_012", # Hispanic or Latino
  "midincome" = "B19013_001", # Median household income in the past 12 months
  "pcincome"  = "B19301_001"  # Per capita income in the past 12 months
)

# Declare geographies we'd like to query
census_geographies <- c(
  "congressional district",
  "county",
  "county subdivision",
  "public use microdata area",
  "school district (elementary)",
  "school district (secondary)",
  "school district (unified)",
  "state legislative district (lower chamber)",
  "state legislative district (upper chamber)",
  "tract"
)

# Folders have a different naming schema than geographies
folders <- c(
  "congressional_district",
  "county",
  "county_subdivision",
  "puma",
  "school_district_elementary",
  "school_district_secondary",
  "school_district_unified",
  "state_representative",
  "state_senate",
  "tract"
)

# Link geographies and folders
folders_df <- data.frame(geography = census_geographies, folder = folders)

# Generate a combination of all years, geographies, and tables
all_combos <- expand.grid(
  geography = census_geographies,
  year = census_years,
  survey = c("acs1", "acs5"),
  stringsAsFactors = FALSE
) |>
  # Join on folder names
  left_join(folders_df) |>
  # Some geographies only exist for the ACS5
  filter(!(survey == "acs1" & geography %in% c(
    "state legislative district (lower chamber)",
    "state legislative district (upper chamber)",
    "tract"
  )))

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
    get_acs(
      geography = geography,
      variables = census_variables,
      survey = survey,
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