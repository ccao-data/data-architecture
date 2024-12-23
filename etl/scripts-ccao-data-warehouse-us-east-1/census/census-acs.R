library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(stringr)
library(tidycensus)
source("utils.R")

# This script retrieves raw ACS data for the data lake
# It populates the warehouse s3 bucket
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "census")

# Retrieve census API key from local .Renviron
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# Declare years we'd like to grab census data for
census_years <-
  Sys.getenv("CENSUS_ACS_MIN_YEAR"):Sys.getenv("CENSUS_ACS_MAX_YEAR")

# Census tables we want to grab. Taken from: https://censusreporter.org/topics/
census_tables <- c(
  "Sex by Age" = "B01001",
  "Median Age by Sex" = "B01002",
  "Race" = "B02001",
  "Hispanic or Latino Origin" = "B03003",
  "Geographical Mobility in the Past Year by Sex for Current Residence in the United States" = "B07003", # nolint
  "Household Type" = "B11001",
  "Sex by Educational Attainment" = "B15002",
  "Poverty Status by Sex by Age" = "B17001",
  "Household Income" = "B19001",
  "Median Household Income" = "B19013",
  "Per Capita Income" = "B19301",
  "Receipt of Food Stamps/SNAP by Poverty Status for Households" = "B22003",
  "Employment Status" = "B23025",
  "Tenure" = "B25003",
  "Median Year Structure Built by Tenure" = "B25037",
  "Median Gross Rent (Dollars)" = "B25064",
  "Median Value (Dollars)" = "B25077",
  "Tenure by Selected Physical and Financial Conditions" = "B25123"
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

# Create a vector of named census variables. These will be used to label
# each output data frame
census_variables_df <- load_variables(2018, "acs5", cache = TRUE)
census_variables <- census_variables_df$label
names(census_variables) <- census_variables_df$name

# Link geographies and folders
folders_df <- data.frame(geography = census_geographies, folder = folders)

# Generate a combination of all years, geographies, and tables
all_combos <- expand.grid(
  geography = census_geographies,
  year = census_years,
  survey = c("acs1", "acs5"),
  stringsAsFactors = FALSE
) %>%
  # Census did not release 2020 acs1 data because of COVID
  filter(year != 2020 | survey != "acs1") %>%
  # Join on folder names
  left_join(folders_df) %>%
  # Some geographies only exist for the ACS5
  filter(!(survey == "acs1" & geography %in% c(
    "state legislative district (lower chamber)",
    "state legislative district (upper chamber)",
    "tract"
  )))

# Function to loop through rows in all_combos, grab census data,
# and write it to a parquet file on S3 if it doesn't already exist
pull_and_write_acs <- function(
    s3_bucket_uri, survey, folder, geography, year, tables = census_tables) {
  remote_file <- file.path(
    s3_bucket_uri, survey,
    paste0("geography=", folder),
    paste0("year=", year),
    paste(survey, folder, paste0(year, ".parquet"), sep = "-")
  )

  # Check to see if file already exists on S3; if it does, skip it
  if (!aws.s3::object_exists(remote_file)) {
    # Print file being written
    message(Sys.time(), " - ", remote_file)

    # These geographies are county-specific rather than state-level
    county_specific <- c("county", "county subdivision", "tract")
    county <- if (geography %in% county_specific) "Cook" else NULL

    # No employment status prior to 2011
    if (year < 2011) {
      tables <- tables[tables != "B23025"]
    }

    # Retrieve specified data from census API
    df <- map_dfc(tables, ~ get_acs(
      geography = geography,
      table = .x,
      survey = survey,
      output = "wide",
      state = "IL",
      county = county,
      year = as.numeric(year),
      cache_table = TRUE
    )) %>%
      rename(any_of(c("GEOID" = "GEOID...1"))) %>%
      select(-starts_with("GEOID..."), -starts_with("NAME")) %>%
      filter(!str_detect(GEOID, "Z"))

    # Write to S3
    arrow::write_parquet(df, remote_file)
  }
}

# Apply function to all_combos
pwalk(all_combos, function(...) {
  df <- tibble::tibble(...)
  pull_and_write_acs(
    s3_bucket_uri = output_bucket,
    survey = df$survey,
    folder = df$folder,
    geography = df$geography,
    year = df$year
  )
}, .progress = TRUE)
