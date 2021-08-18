library(arrow)
library(here)
library(purrr)
library(tidycensus)

# Retrieve Census API key
census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# Get min and max year for retrieved data
cur_year <- format(Sys.Date(), "%Y")
min_year <- as.numeric(Sys.getenv("CENSUS_ACS5_MIN_YEAR", unset = "2010"))
max_year <- as.numeric(Sys.getenv("CENSUS_ACS5_MAX_YEAR", unset = cur_year))

# List of ACS geographies to fetch, see
# https://api.census.gov/data/2011/acs/acs5/geography.html
# or https://walker-data.com/tidycensus/articles/basic-usage.html
# Name is census API name, value is output folder name
geographies <- c(
  "tract" = "tract",
  "state legislative district (upper chamber)" = "state_senate",
  "state legislative district (lower chamber)" = "state_representative",
  "school district (unified)" = "school_district_unified",
  "school district (unified)" = "school_district_secondary",
  "school district (elementary)" = "school_district_elementary",
  "public use microdata area" = "puma",
  "county subdivision" = "county_subdivision",
  "county" = "county",
  "congressional district" = "congressional_district"
)

# List of variables to fetch. See tidycensus::load_variables for a complete list
#
variables <- c(
  "TOTAL_POP" = "B01001_001"
)

# Create data frame of all combinations of years and geographies
grid_df <- expand.grid(
  year = min_year:max_year,
  geography = geographies
) %>%
  dplyr::left_join(geographies)

pmap(
  list(grid_df$year, grid_df$geography, grid_df$file_name),
  function(year, geography, file_name) {
    df <- tidycensus::get_acs(
      geography = names(geography),
      variables = variables,
      output = "wide",
      state = "IL",
      year = year
    )

    write_parquet(
      df,
      sink = here("census", "acs5", file_name, year, ".parquet"),
      compression = "bz2",
      compression_level = 9
    )
  }
)

tidycensus::get_acs(
  geography = "county subdivision",
  variables = variables,
  output = "wide",
  state = "IL",
  year = 2010
)
