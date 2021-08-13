# this script retrieves raw census data for the data lake

# retrieve census API key
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# Illinois PUMAS ----
pumas <- get_acs(
  geography = "public use microdata area",
  variables = "B19013_001",
  state = "IL",
  year = 2019,
  geometry = T
) %>%
  st_transform(3435) %>%
  mutate(PUMA = str_sub(GEOID, 3, 7)) %>%
  select(PUMA, geometry)

# output
sf::write_sf(pumas, here(paste0("shapefiles/raw/pumas_", Sys.Date())), driver = "ESRI Shapefile")
