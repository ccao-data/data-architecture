# this script retrieves raw census data for the data lake

# retrieve census API key
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# ACS ----
# all acs variables we're looking to grab
census_variables <- c("tot_pop"   = "B03002_001",
                      "white"     = "B03002_003",
                      "black"     = "B03002_004",
                      "am_ind"    = "B03002_005",
                      "asian"     = "B03002_006",
                      "pac_isl"   = "B03002_007",
                      "o1"        = "B03002_008",
                      "o2"        = "B03002_009",
                      "o3"        = "B03002_010",
                      "o4"        = "B03002_011",
                      "his"       = "B03002_012",
                      "midincome" = "B19013_001",
                      "pcincome"  = "B19301_001")

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

folders <- data.frame(geography, folder)

# generate a combination of all years, geographies, and tables
all_combos <- expand.grid(geography = geography,
                          year = census_years,
                          survey = c("acs1", "acs5"),
                          stringsAsFactors = FALSE) %>%

  left_join(folders) %>%

  # rearrange
  select(survey, geography, year, folder) %>%

  filter(!(survey == "acs1" & geography %in% c("state legislative district (lower chamber)",
                                               "state legislative district (upper chamber)",
                                               "tract")))

# loop through all the combos and write the data to parquet files
for (i in 1:nrow(all_combos)) {

  # skip a file if it already exists
  if (!file.exists(
    here(paste0("s3-bucket/stable/census/",
                all_combos$survey[i], "/",
                all_combos$folder[i], "/",
                all_combos$survey[i], "_",
                all_combos$year[i], ".parquet")))) {

    print(paste0(Sys.time(), " - s3-bucket/stable/census/",
                 all_combos$survey[i], "/",
                 all_combos$folder[i], "/",
                 all_combos$survey[i], "_",
                 all_combos$year[i], ".parquet"))

    if (all_combos$geography[i] %in% c("county", "county subdivision", "tract")) {

      output <- get_acs(
        geography = all_combos$geography[i],
        variables = census_variables,
        survey = all_combos$survey[i],
        output = "wide",
        state = "IL",
        county = "Cook",
        year = all_combos$year[i],
        cache_table = TRUE
      )

    } else {

      output <- get_acs(
        geography = all_combos$geography[i],
        variables = census_variables,
        survey = all_combos$survey[i],
        output = "wide",
        state = "IL",
        year = all_combos$year[i],
        cache_table = TRUE
      )

    }

    output %>%

      dplyr::rename("geoid" = "GEOID", "geography" = "NAME") %>%

      write_parquet(

        here(paste0("s3-bucket/stable/census/",
                    all_combos$survey[i], "/",
                    all_combos$folder[i], "/",
                    all_combos$survey[i], "_",
                    all_combos$year[i], ".parquet"))

      )

  }

}
