library(stringr)
library(tidycensus)
library(dplyr)
library(purrr)
library(openxlsx)
library(arrow)
library(here)

# retrieve census API key
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# grab a list of all APS 5 year tables ----
census_tables <- openxlsx::read.xlsx(here("census/documentation/2019_DataProductList.xlsx")) %>%
  dplyr::filter(Table.Universe == 'Universe: Total population' & Year == '1,5') %>%
  dplyr::pull(Table.ID)

# remove a couple of 5 year tables tables that don't work with the census API, paste on 1 year table used for pinlocations
census_tables <- c(census_tables[-c(14, 16)], "B19013")

# declare years we'd like to grab census data for
census_years <- 2010:2019
names(census_years) <- 2010:2019

# a function to grab a given table for however many years user would like and stack them into one dataframe
acs_retrieve <- function(table, years) {

  purrr::map_dfr(
    years,
    ~ tidycensus::get_acs(
      geography = "tract",
      table = table,
      output = "wide",
      state = "IL",
      county = "Cook",
      year = .x
    ),
    .id = "year"  # when combining results, add id var (name of list item)
  ) %>%

    # Drop margin of error columns and suffix on estimate columns
    dplyr::select(-ends_with("M", ignore.case = FALSE), -contains("NAME")) %>%
    dplyr::rename_with(~ str_sub(.x, 1, -2), .cols = ends_with("E", ignore.case = FALSE))

}

# apply function to all desired tables
acs <- list()
acs <- lapply(census_tables, acs_retrieve, years = census_years)

# merge tables together by geoid
acs <- Reduce(function(x, y) merge(x, y, all = TRUE), acs)



# # Obtain public use microdata areas from Census API ----
# pumas <- get_acs(
#   geography = "public use microdata area",
#   variables = "B19013_001",
#   state = "IL",
#   year = 2018,
#   geometry = T
# ) %>%
#   st_transform(3435) %>%
#   mutate(PUMA = str_sub(GEOID, 3, 7)) %>%
#   select(PUMA, geometry)

# output
write_parquet(acs, here(paste0("census/acs_tables_", Sys.Date(), ".parquet")))