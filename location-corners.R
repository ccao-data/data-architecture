# Download the street data for Cook county
library(osmdata)
library(tidyverse)
library(sf)
library(sp)
library(ggmap)
library(tidygraph)
library(sfnetworks)
library(dbscan)
library(units)
library(tmap)
library(rgrass)
library(link2GI)
library(nabor)
library(terra)

osm_data <- function(type) {
  opq(bbox = 'evanston illinois', timeout = 1000) %>%
    add_osm_feature(key = 'highway', value = type) %>% 
    add_osm_feature(key = 'highway', value = '!footway') %>% 
    add_osm_feature(key = 'highway:tag', value = '!alley') %>% 
    osmdata_sf()
}

highway_type <- available_tags('highway')

evanston <- lapply(highway_type, osm_data)

#  Combine items in one list
# line <- cook_county[[1]]$osm_lines
# for (i in 2:length(highway_type)){
#   if (is_null(cook_county[[i]]$osm_lines)){
#     i + 1} 
#   else{
#   line <- dplyr::bind_rows(line, cook_county[[i]]$osm_lines)
#   }
# }

evanston_center <- purrr::map_dfr(evanston, ~ .x$osm_lines) %>%
  filter(!highway %in% c('bridleway', 'construction', 'corridor', 'cycleway', 'elevator', 'service', 'services', 'steps'))

plot(evanston_center$geometry)

# Construct the Evanston Network 
network <- as_sfnetwork(evanston_center, directed = FALSE) %>% 
  activate(edges) %>%
  arrange(edge_length()) %>%
  filter(!edge_is_multiple()) %>%
  filter(!edge_is_loop()) %>% 
  st_transform(3435) %>%
  convert(to_spatial_simple) %>% 
  convert(to_spatial_smooth) %>% 
  convert(to_spatial_subdivision) %>% 
  activate(nodes) %>% 
  mutate(degree = centrality_degree())
  
plot(network)

# Find the corner unit and plot it 
corner <- network %>% 
  filter(degree >= 3) %>% 
  st_as_sf()

ggplot() + 
  geom_sf(data = st_as_sf(network, "edges"), col = "grey50") +
  geom_sf(data = st_as_sf(network, "nodes"), aes(col = factor(degree))) +
  theme_void()

# Buffer approach
buffer_area <- st_buffer(corner$geometry, dist = 100)

# find out whether each parcel touches the buffer area
parcels <- st_read("https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?MUNICIPALITY=Evanston&$limit=50000") %>% 
  st_transform(3435) %>% 
  mutate(intersect_buffer = st_overlaps(parcels, buffer_area)) %>% 
  mutate(sel_logical = lengths(intersect_buffer) > 0) %>% 
  mutate(num_buffer = lengths(intersect_buffer))

corner_parcels <- parcels %>% 
  filter(num_buffer == 1)

ggplot() + 
  geom_sf(data = st_geometry(parcels)) + 
  geom_sf(data = buffer_area, col = 'red') + 
  geom_sf(data = st_geometry(corner_parcels), fill = 'blue') + 
  theme_void()

# find out whether each buffer area includes any parcel
corner <- corner %>% 
  mutate(buffer_area = st_buffer(geometry, dist = 100)) %>% 
  mutate(parcel_within = st_overlaps(buffer_area, parcels))


# function to increase buffer distance if no overlap between buffer area and parcels
buffer_parcel <- function(buffer_dis){
  corner <- corner %>% 
    mutate(buffer_area = st_buffer(geometry, dist = buffer_dis)) %>% 
    mutate(parcel_within = st_overlaps(buffer_area, parcels))
  
  for(i in (1:nrow(corner))){
    while(lengths(corner$parcel_within[i]) == 0){
      buffer_dis = buffer_dis + 5
      corner <- corner %>% 
        mutate(buffer_area = st_buffer(geometry, dist = buffer_dis)) %>% 
        mutate(parcel_within = st_overlaps(buffer_area, parcels))
    }
  }
  
}

buffer_parcel(70)

#############
