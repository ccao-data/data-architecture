# this script retrieves raw census data for the data lake

# retrieve census API key
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# ACS ----
# grab a list of all ACS 5 year tables
five_year_census_tables <- openxlsx::read.xlsx(here("census/documentation/2019_DataProductList.xlsx"), sheet = "2019 Data Product List") %>%
  dplyr::filter(Table.Universe == 'Universe: Total population' & Year == '1,5') %>%
  dplyr::pull(Table.ID)

# remove a couple of 5 year tables tables that don't work with the census API
five_year_census_tables <- five_year_census_tables[-c(14, 16)]

# 1 year tables
one_year_census_tables <- c(
  "B25002", # OCCUPANCY STATUS
  "B19083", # GINI INDEX OF INCOME INEQUALITY
  "B25013", # TENURE BY EDUCATIONAL ATTAINMENT OF HOUSEHOLDER
  "B17019", # POVERTY STATUS IN THE PAST 12 MONTHS OF FAMILIES BY HOUSEHOLD TYPE BY TENURE
  "B22003", # RECEIPT OF FOOD STAMPS/SNAP IN THE PAST 12 MONTHS BY POVERTY STATUS IN THE PAST 12 MONTHS FOR HOUSEHOLDS
  "B19013", # MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2019 INFLATION-ADJUSTED DOLLARS)
  "B25070", # GROSS RENT AS A PERCENTAGE OF HOUSEHOLD INCOME IN THE PAST 12 MONTHS
  "B25068"  # BEDROOMS BY GROSS RENT
)

# declare years we'd like to grab census data for
census_years <- 2010:2019

# declare geographies we'd like to query
geographies <- c(
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

# generate a combination of all years, geographies, and tables
all_combos <- expand.grid(geography = geographies,
                          year = census_years,
                          table = c(one_year_census_tables, five_year_census_tables),
                          stringsAsFactors = FALSE) %>%

  # note which tables are acs1 vs acs5 for API
  mutate(survey = case_when(table %in% one_year_census_tables ~ "acs1",
                            table %in% five_year_census_tables ~ "acs5")) %>%

  # rearrange
  select(survey, geography, year, table) %>%

  filter(!(survey == "acs1" & geography %in% c("state legislative district (lower chamber)",
                                             "state legislative district (upper chamber)",
                                             "tract")))

# loop through all the combos and write the data to parquet files
for (i in 1:nrow(all_combos)) {

  # skip a file if it already exists
  if (!file.exists(
    here(paste0("census/raw/",
                all_combos$survey[i], "/",
                all_combos$geography[i], "/",
                all_combos$table[i], "_",
                all_combos$year[i], ".parquet")))) {

    print(paste0("census/raw/",
                 all_combos$survey[i], "/",
                 all_combos$geography[i], "/",
                 all_combos$table[i], "_",
                 all_combos$year[i], ".parquet"))

    if (all_combos$geography[i] %in% c("county", "county subdivision", "tract")) {

      output <- tidycensus::get_acs(
        geography = all_combos$geography[i],
        table = all_combos$table[i],
        survey = all_combos$survey[i],
        output = "wide",
        state = "IL",
        county = "Cook",
        year = all_combos$year[i],
        cache_table = TRUE
      )

    } else {

      output <- tidycensus::get_acs(
        geography = all_combos$geography[i],
        table = all_combos$table[i],
        survey = all_combos$survey[i],
        output = "wide",
        state = "IL",
        year = all_combos$year[i],
        cache_table = TRUE
      )

    }

    output %>%

      # Drop margin of error columns and suffix on estimate columns
      dplyr::select(-ends_with("M", ignore.case = FALSE), -contains("NAME")) %>%
      dplyr::rename_with(~ str_sub(.x, 1, -2), .cols = ends_with("E", ignore.case = FALSE)) %>%

      write_parquet(

        here(paste0("census/raw/",
                    all_combos$survey[i], "/",
                    all_combos$geography[i], "/",
                    all_combos$table[i], "_",
                    all_combos$year[i], ".parquet"))

      )

  }

}
