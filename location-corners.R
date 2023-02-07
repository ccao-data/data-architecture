# Download the street data for Cook county
library(osmdata)
library(tidyverse)
library(sf)
library(ggmap)
library(tidygraph)
library(sfnetworks)
library(dbscan)

osm_data <- function(type) {
  opq(bbox = 'cook county illinois', timeout = 1000) %>%
    add_osm_feature(key = 'highway', value = type) %>% 
    add_osm_feature(key = 'highway', value = '!footway') %>% 
    add_osm_feature(key = 'highway:tag', value = '!alley') %>% 
    osmdata_sf()
}

highway_type <- available_tags('highway')

cook_county <- lapply(highway_type, osm_data)


# Combine items in one list
line <- cook_county[[1]]$osm_lines
for (i in 2:length(highway_type)){
  if (is_null(cook_county[[i]]$osm_lines)){
    i + 1
  } else{
  line <- st_join(cook_couty[[i]]$osm_lines, line)
  }
}

# Construct the network
network <- as_sfnetwork(line, directed = FALSE)

degree <- network %>% 
  activate(nodes) %>% 
  mutate(degree = centrality_degree()) %>% 
  filter(degree >= 3)


