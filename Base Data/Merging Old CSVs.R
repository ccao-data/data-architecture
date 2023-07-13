
   
township <- "Barrington"

bbox <- ccao::town_shp %>%
  filter(township_name == township) %>%
  st_bbox()

#bbox <- c(left = -87.9401, bottom = 41.6445, right = -87.5244, top = 42.023)



# Street network data
osm_data <- function(type) {
  opq(bbox = bbox) %>%
    add_osm_feature(key = "highway", value = type) %>%
    add_osm_feature(key = "highway", value = "!footway") %>%
    add_osm_feature(key = "highway:tag", value = "!alley") %>%
    osmdata_sf()
}

highway_type <- available_tags("highway")

town_osm <- lapply(highway_type, osm_data)

town_osm_center <- town_osm$Value$osm_lines %>%
  filter(!highway %in% c("bridleway", "construction", "corridor", "cycleway", "elevator", "service", "services", "steps"))


# Construct the street network
network <- suppressWarnings({
  as_sfnetwork(town_osm_center, directed = FALSE) %>%
    activate(edges) %>%
    arrange(edge_length()) %>%
    filter(!edge_is_multiple()) %>%
    filter(!edge_is_loop()) %>%
    st_transform(3435) %>%
    convert(to_spatial_simple) %>%
    convert(to_spatial_subdivision) %>%
    activate(nodes) %>%
    mutate(degree = centrality_degree())
})


parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>%
  mutate(id = row_number()) %>%
  select(pin10, geometry)



corner <- read_csv("~/corner_lots_results/csv/corner_barrington.csv") %>%
  select(pin10) %>%
  mutate(cornerlot = 1)


parcels$pin10 <- as.numeric(parcels$pin10)
corner$pin10 <- as.numeric(corner$pin10)

parcels$duplicated <- parcels$pin10 %>%
  duplicated()

parcels <- parcels %>%
  subset(duplicated == FALSE)

final_corner <- right_join(corner, parcels, by = "pin10")

final_corner$cornerlot <- ifelse(final_corner$cornerlot == 1, 1, 0)

final_corner[is.na(final_corner)] <- 0

table(final_corner$cornerlot)

final_corner <- st_as_sf(final_corner)




directory <- "Base Data"
file_name <- paste0(township, ".shp")

# Create the complete file path
output_file <- file.path(directory, file_name)

sf::st_write(final_corner, output_file, append = FALSE)

file_name <- paste0("Streets", township, ".shp")

output_file <- file.path(directory, file_name)


sf_graph_edges <- network %>% activate(edges) %>% as_tibble() %>% st_as_sf()

sf_graph_edges <- sf_graph_edges %>%
  select(geometry)



sf::st_write(sf_graph_edges, output_file, append = FALSE)






 ggplot() +
   geom_sf(data = final_corner, aes(fill = cornerlot)) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')





# 
# 
# corner_bloom <- read_csv("~/corner_lots_results/csv/corner_bloom.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# 
# corner_bremen <- read_csv("~/corner_lots_results/csv/corner_bremen.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# 
# corner_calumet <- read_csv("~/corner_lots_results/csv/corner_cicero.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# 
# corner_cicero <- read_csv("~/corner_lots_results/csv/corner_elk_grove.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_elk_grove <- read_csv("~/corner_lots_results/csv/corner_evanston.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# 
# corner_evanston <- read_csv("~/corner_lots_results/csv/corner_evanston.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# corner_hanover <- read_csv("~/corner_lots_results/csv/corner_hanover.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)


# corner_hyde_park <- read_csv("~/corner_lots_results/csv/corner_hyde_park.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_lake_view <- read_csv("~/corner_lots_results/csv/corner_lake_view.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# corner_lemont <- read_csv("~/corner_lots_results/csv/corner_lemont.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
corner_leyden <- read_csv("~/corner_lots_results/csv/corner_leyden.csv")%>%
  select(pin10) %>%
  mutate(cornerlot = 1)
# 
# corner_lyons <- read_csv("~/corner_lots_results/csv/corner_lyons.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_maine <- read_csv("~/corner_lots_results/csv/corner_maine.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_new_trier <- read_csv("~/corner_lots_results/csv/corner_new_trier.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_niles <- read_csv("~/corner_lots_results/csv/corner_niles.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

corner_north <- read_csv("~/corner_lots_results/csv/corner_north.csv")%>%
  select(pin10) %>%
  mutate(cornerlot = 1)

# corner_northfield <- read_csv("~/corner_lots_results/csv/corner_northfield.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_oak_park <- read_csv("~/corner_lots_results/csv/corner_oak_park.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_orland <- read_csv("~/corner_lots_results/csv/corner_orland.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_palatine <- read_csv("~/corner_lots_results/csv/corner_palatine.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_palos <- read_csv("~/corner_lots_results/csv/corner_palos.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_proviso <- read_csv("~/corner_lots_results/csv/corner_proviso.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# corner_rich <- read_csv("~/corner_lots_results/csv/corner_rich.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_river_forest <- read_csv("~/corner_lots_results/csv/corner_river_foreset.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_river_side <- read_csv("~/corner_lots_results/csv/corner_river_side.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)
# 
# corner_rogers_park <- read_csv("~/corner_lots_results/csv/corner_rogers_park.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_schaumburg <- read_csv("~/corner_lots_results/csv/corner_schaumburg.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_stickney <- read_csv("~/corner_lots_results/csv/corner_stickney.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_thornton <- read_csv("~/corner_lots_results/csv/corner_thornton.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)

# corner_wheeling <- read_csv("~/corner_lots_results/csv/corner_wheeling.csv")%>%
#   select(pin10) %>%
#   mutate(cornerlot = 1)