library(arrow)
library(dplyr)
library(here)
library(tidycensus)

# this script retrieves raw decennial census data for the data lake

# retrieve census API key
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# all census variables we're looking to grab
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

# declare years we'd like to grab census data for
census_years <- Sys.getenv("CENSUS_DEC_MIN_YEAR")

# declare geographies we'd like to query
geography <- c(
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

# folders have a different naming schema than goegraphies
folder <- c(
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

# link geographies and folders
folders <- data.frame(geography, folder)

# generate a combination of all years, geographies, and tables
all_combos <- expand.grid(
  geography = geography,
  year = census_years,
  survey = "decennial",
  stringsAsFactors = FALSE
) |>
  # join on folder names
  left_join(folders)

# function to loop through rows in all_combos, grab census data, and write it to a parquet file
pull_and_write_decennial <- function(x) {
  survey <- x["survey"]
  folder <- x["folder"]
  geography <- x["geography"]
  year <- x["year"]

  current_file <- paste0(
    "s3-bucket/stable/census/",
    survey, "/",
    folder, "/",
    year, ".parquet"
  )

  # check to see if file already exists; if it does, skip it
  if (!file.exists(here(current_file))) {

    # print file being written
    print(
      paste0(Sys.time(), " - ", current_file)
    )

    # these geographies are county-specific rather than state-level
    county <- if (geography %in% c("block", "block group", "county", "county subdivision", "tract")) "Cook" else NULL

    # retrieve specified data from census API
    get_decennial(
      geography = geography,
      variables = census_variables,
      output = "wide",
      state = "IL",
      county = county,
      year = as.numeric(year),
      cache_table = TRUE
    ) |>
      # clean output, write to parquet files
      dplyr::rename("geoid" = "GEOID", "geography" = "NAME") |>
      arrow::write_parquet(here(current_file))
  }
}

# apply function to all_combos
apply(all_combos, 1, pull_and_write_decennial)

# clean
rm(list = ls())