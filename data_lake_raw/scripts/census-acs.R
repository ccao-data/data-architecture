# this script retrieves raw ACS data for the data lake

# retrieve census API key
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# all acs variables we're looking to grab
census_variables <- c(
  "tot_pop"   = "B03002_001", # Total population
  "white"     = "B03002_003", # White alone
  "black"     = "B03002_004", # Black or African American alone
  "am_ind"    = "B03002_005", # American Indian and Alaska Native alone
  "asian"     = "B03002_006", # Asian alone
  "pac_isl"   = "B03002_007", # Native Hawaiian and Other Pacific Islander alone
  "o1"        = "B03002_008", # Some other race alone
  "o2"        = "B03002_009", # Two or more races
  "o3"        = "B03002_010", # Two races including Some other race
  "o4"        = "B03002_011", # Two races excluding Some other race, and three or more races
  "his"       = "B03002_012", # Hispanic or Latino
  "midincome" = "B19013_001", # Median household income in the past 12 months
  "pcincome"  = "B19301_001"  # Per capita income in the past 12 months
)

# declare years we'd like to grab census data for
census_years <- Sys.getenv("CENSUS_ACS_MIN_YEAR"):Sys.getenv("CENSUS_ACS_MAX_YEAR")

# declare geographies we'd like to query
geography <- c(
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

# folders have a different naming schema than goegraphies
folder <- c(
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

# link geographies and folders
folders <- data.frame(geography, folder)

# generate a combination of all years, geographies, and tables
all_combos <- expand.grid(geography = geography,
                          year = census_years,
                          survey = c("acs1", "acs5"),
                          stringsAsFactors = FALSE) |>

  # join on folder names
  left_join(folders) |>

  # some geographies only exist for the as5
  filter(!(survey == "acs1" & geography %in% c("state legislative district (lower chamber)",
                                               "state legislative district (upper chamber)",
                                               "tract")))

# function to loop through rows in all_combos, grab census data, and write it to a parquet file
pull_and_write_acs <- function(x) {

  survey    <- x["survey"]
  folder    <- x["folder"]
  geography <- x["geography"]
  year      <- x["year"]

  current_file <- paste0("s3-bucket/stable/census/",
                         survey, "/",
                         folder, "/",
                         survey, "_",
                         year, ".parquet")

  # check to see if file already exists; if it does, skip it
  if (!file.exists(here(current_file))) {

    # print file being written
    print(
      paste0(Sys.time(), " - ", current_file)
    )

    # these geographies are county-specific rather than state-level
    county <- if (geography %in% c("county", "county subdivision", "tract")) "Cook" else NULL

    # retrieve specified data from census API
    get_acs(
      geography = geography,
      variables = census_variables,
      survey = survey,
      output = "wide",
      state = "IL",
      county = county,
      year = year,
      cache_table = TRUE
    ) |>

      # clean output, write to parquet files
      dplyr::rename("geoid" = "GEOID", "geography" = "NAME") |>
      arrow::write_parquet(here(current_file))

  }

}

# apply function to all_combos
apply(all_combos, 1, pull_and_write_acs)