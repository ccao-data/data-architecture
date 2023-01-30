# Download the street data for Cook county
library(osmdata)
library(tidyverse)
library(sf)
library(ggmap)

osm_data <- function(type) {
  opq(bbox = 'cook county illinois', timeout = 1000) %>%
    add_osm_feature(key = 'highway', value = type) %>% 
    add_osm_feature(key = 'highway:tag', value = '!alley') %>% 
    osmdata_sf()
}

highway_type <- available_tags('highway')

# highway_type <- c('mototway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential',
                  'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link')

cook_county <- lapply(highway_type, osm_data)


