# Download the street data for Cook county
library(osmdata)
library(tidyverse)
library(sf)
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
library(nngeo)
library(geosphere)

osm_data <- function(type) {
  opq(bbox = 'evanston illinois', timeout = 1000) %>%
    add_osm_feature(key = 'highway', value = type) %>% 
    add_osm_feature(key = 'highway', value = '!footway') %>% 
    add_osm_feature(key = 'highway:tag', value = '!alley') %>% 
    osmdata_sf()
}

highway_type <- available_tags('highway')

evanston <- lapply(highway_type, osm_data)

evanston_center <- purrr::map_dfr(evanston, ~ .x$osm_lines) %>% 
  filter(!highway %in% c('bridleway', 'construction', 'corridor', 'cycleway', 'elevator', 'service', 'services', 'steps'))


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
  st_transform(3435) 

parcels <- parcels %>% 
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

# Crossing approach
parcels <- parcels %>% 
  mutate(centriod = st_centroid(geometry)) %>% 
  mutate(min_rectangle = st_minimum_rotated_rectangle(geometry))

# Step 1: Create the minimum rectangle
min_rectangle_line <- st_segments(parcels$min_rectangle) %>% 
  st_transform(crs = 4326) 

# Step 2: Draw the cross
## find out the bearing angle
rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>% 
  activate(edges) %>%
  mutate(bearing = edge_azimuth()) %>% 
  mutate(bearing = units::set_units(bearing, 'degree')) %>% 
  mutate(group_id = rep(1:16725, each = 4)) %>% 
  mutate(id = row_number()) %>% 
  mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, 'degree'))) %>% 
  st_as_sf() 

## find out distance
rectangle_network <- rectangle_network %>% 
  mutate(length = st_length(x) * 2/3 + units::set_units(5, 'm'))

## find out starting point
centriod <- st_centroid(parcels$geometry) %>% st_as_sf()
centriod <- centriod %>% 
  slice(rep(1:n(), each = 4)) %>% 
  st_transform(crs = 4326) %>% 
  st_coordinates() 

## draw the cross
dest <- destPoint(p = centriod, b = rectangle_network$corrected_bearing, d = rectangle_network$length)

cross <- cbind(centriod, dest) %>% as.data.frame()

draw_line <- function(r){st_linestring(t(matrix(unlist(r), 2, 2)))}

cross$geom = st_sfc(sapply(1:nrow(cross), 
                        function(i){draw_line(cross[i,])},simplify=FALSE))

cross <- cross %>% 
  mutate(group_id = rep(1:16725, each = 4)) %>% 
  mutate(id = 1:66900)

crossing <- st_set_crs(cross$geom, 4326)
cross$geom <- st_set_crs(cross$geom, 4326)
parcels_new <- st_transform(parcels$min_rectangle, 4326)

# visualization 
ggplot() + 
  geom_sf(data = parcels_new) +
  geom_sf(data = crossing) + 
  theme_void()

# Find out all the units touched by the cross
touching_units <- cross %>% #lessons learned: do not use st_touches or st_overlaps; should use st_intersects
  mutate(touching_unit = st_intersects(geom, parcels_new)) %>% 
  mutate(number_touching_unit = lengths(touching_unit))


# Step 3: Find out all the neighbor units for a parcel
neighbor_units <- parcels %>% 
  mutate(neighbor_unit = st_touches(geometry, geometry)) %>% 
  mutate(contain_unit = st_contains(geometry, geometry)) %>% 
  select(geometry, neighbor_unit, contain_unit) %>% 
  mutate(group_id = 1:16725) 

# Step 4: Find out the complement for {cross_touching_unit & neighbor_unit}
cross_corner_unit <- touching_units %>% 
  left_join(neighbor_units, by = 'group_id') 

touching_unit <- cross_corner_unit$touching_unit
neighbor_unit <- cross_corner_unit$neighbor_unit
contain_unit <- cross_corner_unit$contain_unit

neighbor <- c()
cross_unit <- c()
for (i in 1:length(touching_unit)){
  neighbor[[i]] = setdiff(touching_unit[[i]], contain_unit[[i]])
  cross_unit[[i]] = setdiff(neighbor[[i]], neighbor_unit[[i]])
}

cross_id <- which(lengths(cross_unit) != 0)

corner_cross <- cross %>% 
  cbind(rectangle_network$corrected_bearing) %>% 
  rename('bearing' = 'rectangle_network$corrected_bearing') %>% 
  filter(id %in% cross_id) %>% 
  group_by(group_id) %>% 
  mutate(n = n()) %>% 
  filter(n >= 2) %>% 
  mutate(diff_degree = bearing - lag(bearing)) %>%
  filter(!any(diff_degree >= 179 & diff_degree <= 181, na.rm = TRUE) & !any(diff_degree >= -181 & diff_degree <= -179, na.rm = TRUE) )

ggplot() + 
  geom_sf(data = parcels_new) +
  geom_sf(data = corner_cross$geom, color = 'red') + 
  theme_void()

parcel_id <- unique(corner_cross$group_id)

corner_parcel <- parcels %>% 
  mutate(id = 1:16725) %>% 
  filter(id %in% parcel_id)


ggplot() + 
  geom_sf(data = st_geometry(parcels)) + 
  #geom_sf(data = crossing, col = 'red') + 
  geom_sf(data = st_geometry(corner_parcel), fill = 'blue') + 
  theme_void()
  
##################

# create the list of group_id
n <- 4 # number of times each number is repeated
m <- 16725*n # total number of elements in the list
group_id <- vector("list", m) # initialize a list of length m

# Generate the elements of the list using a loop
for (i in 1:m) {
  if (i %% n == 1) {
    group_id[[i]] <- (i-1)/n + 1
  } else {
    group_id[[i]] <- group_id[[i-1]]
  }
}

