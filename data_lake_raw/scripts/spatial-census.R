library(dplyr)
library(here)
library(purrr)
library(sf)
library(tigris)

# Get min and max year for retrieved data
cur_year <- format(Sys.Date(), "%Y")
min_year <- as.numeric(Sys.getenv("CENSUS_GEO_MIN_YEAR", unset = "2010"))
max_year <- as.numeric(Sys.getenv("CENSUS_GEO_MAX_YEAR", unset = cur_year))
years <- min_year:max_year

# Default Census geodata path
geo_path <- here("data", "spatial", "census")

# BLOCK GROUP
map(years, function(year) {
  path <- here(geo_path, "block_group", paste0(year, ".geojson"))

  df <- block_groups(state = "17", year = year) %>%
    filter(COUNTYFP == "031")  # Keep only Cook County

  st_write(df, path, delete_dsn = TRUE)
})
