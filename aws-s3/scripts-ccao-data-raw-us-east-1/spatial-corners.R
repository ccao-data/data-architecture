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

# Street network data
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

# Construct the street network
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

# Parcel data
parcels <- st_read("https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?MUNICIPALITY=Evanston&$limit=50000") %>% 
  st_transform(3435)

parcels <- parcels %>% mutate(id = 1:nrow(parcels))

# Create a nested list for target parcel, neighbor parcel, neighbor network
## Find the neighbor parcel clip
parcels <- parcels %>% 
  mutate(buffer_area = st_buffer(geometry, dist = units::set_units(50, 'm'))) %>% 
  mutate(clip_parcel = st_intersects(parcels, buffer_area))

clip_parcel <- map(parcels$clip_parcel, function(x){
  parcels$geometry[x]
})

## Find the neighbor network clip
clip_network <- c()
for(i in 1:nrow(parcels)){
  clip_network[[i]] <- network %>% 
    activate("edges") %>%
    st_filter(parcels$buffer_area[i], .pred = st_overlaps) %>%
    activate("nodes") %>%
    filter(!node_is_isolated())
}

parcel <- parcels$geometry


### Crossing approach
# Set sf_use_s2(FALSE)

crossing <- function(parcel, network, neighbor_parcel){
  
  # Step 1: Create the minimum rectangle
  min_rectangle = st_minimum_rotated_rectangle(parcel[i])
  min_rectangle_line <- st_segments(min_rectangle) %>% 
    st_transform(crs = 4326)
  
  # Step 2: Draw the cross
  ## find out the bearing angle
  rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>% 
    activate(edges) %>%
    mutate(bearing = edge_azimuth()) %>% 
    mutate(bearing = units::set_units(bearing, 'degree')) %>% 
    #mutate(group_id = rep(1:nrow(parcels), each = 4)) %>% 
    mutate(id = row_number()) %>% 
    mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, 'degree'))) %>% 
    st_as_sf() 
  
  ## find out distance
  rectangle_network <- rectangle_network %>% 
    mutate(length = st_length(x)+units::set_units(10,'m'))
  
  ## find out starting point
  centriod <- st_centroid(parcel[i]) %>% st_as_sf()
  centriod <- centriod %>% 
    slice(rep(1:n(), each = 4)) %>% 
    st_transform(crs = 4326) %>% 
    st_coordinates() 
  
  ## draw the cross
  dest <- destPoint(p = centriod, b = rectangle_network$corrected_bearing, d = rectangle_network$length)
  
  cross <- cbind(centriod, dest) %>% as.data.frame()
  
  draw_line <- function(r){st_linestring(t(matrix(unlist(r), 2, 2)))}
  
  cross$geom = st_sfc(sapply(1:nrow(cross), 
                             function(i){draw_line(cross[i,])},simplify = FALSE))
  
  cross <- cross %>% 
    # mutate(group_id = rep(1:nrow(parcels), each = 4)) %>% 
    # mutate(id = 1:(nrow(parcels)*4)) %>% 
    mutate(length = st_length(geom)) %>% 
    # group_by(group_id) %>% 
    mutate(aspect_ratio = lag(length)/length) 
  
  cross$geom <- st_set_crs(cross$geom, 4326)
  parcels_new <- st_transform(neighbor_parcel[[i]], 4326)
  street_network <- network[[i]] %>%
    activate('edges') %>%
    st_as_sf() %>%
    st_transform(4326)
  
  # Find out all the units touched by the cross
  touching_unit <- st_intersects(cross$geom, parcels_new)
  
  # Step 3: Find out all the neighbor units for a parcel
  neighbor_unit <- st_touches(parcel[i], neighbor_parcel[[i]])
  neighbor_unit <- rep(neighbor_unit, each = 4)
  
  # Step 4: Find out the intersection of {cross_touching_unit & neighbor_unit}
  neighbor_and_touching <- list()
  for (i in 1:4){
    neighbor_and_touching[[i]] = intersect(touching_unit[[i]], neighbor_unit[[i]])
  }
  
  # print(touching_unit)
  # print(neighbor_unit)
  # print(neighbor_and_touching)
  
  # Step 5: Remove the cross segments in the intersection of {cross_touching_unit & neighbor_unit}
  
  cross_int <- imap_lgl(neighbor_and_touching, function(x, i) {
    any(map_lgl(x, function(y) {
      as.logical(st_intersects(cross$geom[i], parcels_new[[y]]))
    }))
  })
  
  cross_filter <- cross %>% 
    mutate(is_neighboring = cross_int) %>%
    mutate(id = 1:4) %>% 
    filter(is_neighboring == FALSE)
  
  # Step 6: Calculate the angle of cross segments 
  angle_diff <- function(theta1, theta2){
    theta <- abs(theta1 - theta2) %% 360 
    return(ifelse(theta > 180, 360 - theta, theta))
  }
  
  cross_corner <- rectangle_network %>% 
    mutate(bearing = as.numeric(bearing)) %>% 
    filter(id %in% cross_filter$id) %>% 
    mutate(diff_degree = angle_diff(bearing, lag(bearing))) %>% 
    filter(diff_degree >= 85 & diff_degree <= 95, na.rm = TRUE) 
  
  touching_unit_street <- st_intersects(cross$geom, street_network)
  touching_street_number <- sum(lengths(touching_unit_street))
  
  #   left_join(cross_corner_unit, by = 'id') %>% 
  #   select(id, group_id.x, x, diff_degree, touching_street) %>% 
  #   group_by(group_id.x) %>% 
  #   mutate(n = n()) %>% 
  #   mutate(dummy_street = ifelse(lengths(touching_street) == 0, 0, 1)) %>% 
  #   filter(sum(dummy_street) >= 2) 
  # 
  # parcel_id <- unique(cross_corner$group_id.x)
  cross_corner_number <- nrow(cross_corner)
  
  corner_lot_indicator <- c()
  
  if (touching_street_number >= 2 & cross_corner_number >= 1){
    return (TRUE)
  } else {
    return(FALSE)
  }
}

corner_indicator <- c()

for (i in 1:nrow(parcels)){
  corner_indicator[[i]] <- crossing(parcel, clip_network, clip_parcel)
}

corner_parcel <- parcels %>% 
  mutate(corner_indicator = unlist(corner_indicator)) %>% 
  filter(corner_indicator == TRUE)


ggplot() + 
  geom_sf(data = st_geometry(parcels)) + 
  geom_sf(data = st_geometry(corner_parcel), fill = 'blue') +
  #geom_sf(data = st_as_sf(network, 'edges'), col = 'green') + 
  theme_void()

###################################################
# parcels <- parcels %>%
#    mutate(min_rectangle = st_minimum_rotated_rectangle(geometry))
# 
#  # Step 1: Create the minimum rectangle
#  min_rectangle_line <- st_segments(parcels$min_rectangle) %>%
#    st_transform(crs = 4326)
# 
#  # Step 2: Draw the cross
#  ## find out the bearing angle
#  rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>%
#    activate(edges) %>%
#    mutate(bearing = edge_azimuth()) %>%
#    mutate(bearing = units::set_units(bearing, 'degree')) %>%
#    mutate(group_id = rep(1:nrow(parcels), each = 4)) %>%
#    mutate(id = row_number()) %>%
#    mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, 'degree'))) %>%
#    st_as_sf()
# #
# # ## find out distance
#  rectangle_network <- rectangle_network %>%
#    mutate(length = st_length(x)+units::set_units(10,'m'))
# #
# # ## find out starting point
#  centriod <- st_centroid(parcels$geometry) %>% st_as_sf()
#  centriod <- centriod %>%
#       slice(rep(1:n(), each = 4)) %>%
#    st_transform(crs = 4326) %>%
#    st_coordinates()
# 
# ## draw the cross
#  dest <- destPoint(p = centriod, b = rectangle_network$corrected_bearing, d = rectangle_network$length)
# #
#  cross <- cbind(centriod, dest) %>% as.data.frame()
# #
#  draw_line <- function(r){st_linestring(t(matrix(unlist(r), 2, 2)))}
# #
#  cross$geom = st_sfc(sapply(1:nrow(cross),
#                             function(i){draw_line(cross[i,])},simplify = FALSE))
# #
#  cross <- cross %>%
#    mutate(group_id = rep(1:nrow(parcels), each = 4)) %>%
#    mutate(id = 1:(nrow(parcels)*4)) %>%
#    mutate(length = st_length(geom)) %>%
#    group_by(group_id) %>%
#    mutate(aspect_ratio = lag(length)/length) %>%
#    mutate(buffer_cross_area = st_buffer(geom, dist = 5))
# 
#  cross %>%
#    mutate(cross_buffer = st_buffer(geom, dist = 5))

# Manually filter out those long units
long_unit_id <- cross %>%
  group_by(group_id) %>%
  filter(aspect_ratio >= 30) %>%
  select(group_id)

# crossing <- st_set_crs(cross$geom, 4326)
# cross$geom <- st_set_crs(cross$geom, 4326)
# parcels_new <- st_transform(parcels$min_rectangle, 4326)
# street_network <- network %>%
#   activate('edges') %>%
#   st_as_sf() %>%
#   st_transform(4326)
#
# # visualization of the cross
# ggplot() +
#   geom_sf(data = parcels_new) +
#   geom_sf(data = crossing) +
#   geom_sf(data = st_as_sf(network, 'edges'), col = 'blue') +
#   theme_void()

# Find out all the units touched by the cross
touching_units <- cross %>% # lessons learned: do not use st_touches or st_overlaps; should use st_intersects
  mutate(touching_unit = st_intersects(geom, parcels_new)) %>%
  #mutate(touching_street = st_intersects(geom, street_network)) #find out whether the segment of a cross touches a street
  
  # Remember to set: sf_use_s2(FALSE)
  
  # Step 3: Find out all the neighbor units for a parcel
  # neighbor_units <- parcels %>%
  #   mutate(neighbor_unit = st_touches(geometry, geometry)) %>%
  #   # mutate(neighbor_unit = st_overlaps(buffer_parcel, buffer_parcel)) %>%
  #   select(geometry, neighbor_unit) %>%
  #   mutate(group_id = 1:nrow(parcels))
  

# # Step 4: Find out the intersection of {cross_touching_unit & neighbor_unit}
# cross_corner_unit <- touching_units %>%
#   left_join(neighbor_units, by = 'group_id')
#
# touching_unit <- cross_corner_unit$touching_unit
# neighbor_unit <- cross_corner_unit$neighbor_unit
#
# neighbor_and_touching <- list()
#
# for (i in 1:length(touching_unit)){
#   neighbor_and_touching[[i]] = intersect(touching_unit[[i]], neighbor_unit[[i]])
# }
#
# # Step 5: Remove the cross segments in the intersection of {cross_touching_unit & neighbor_unit}
# cross_int <- imap_lgl(neighbor_and_touching, function(x, i) {
#   any(map_lgl(x, function(y) {
#     as.logical(st_intersects(cross$geom[i], parcels_new[[y]]))
#   }))
# })

# cross_filter <- cbind(cross, cross_int) %>%
#   rename("is_neighboring" = '...11') %>%
#   filter(is_neighboring == FALSE)

# Step 6: Calculate the angle of cross segments
# angle_diff <- function(theta1, theta2){
#   theta <- abs(theta1 - theta2) %% 360
#   return(ifelse(theta > 180, 360 - theta, theta))
# }
#
# cross_corner <- rectangle_network %>%
#   mutate(bearing = as.numeric(bearing)) %>%
#   filter(id %in% cross_filter$id) %>%
#   group_by(group_id) %>%
#   mutate(n = n()) %>%
#   filter(n >= 2) %>%
#   mutate(diff_degree = angle_diff(bearing, lag(bearing))) %>%
#   filter(any(diff_degree >= 85 & diff_degree <= 95, na.rm = TRUE)) %>%
#   left_join(cross_corner_unit, by = 'id') %>%
#   select(id, group_id.x, x, diff_degree, touching_street) %>%
#   group_by(group_id.x) %>%
#   mutate(n = n()) %>%
#   mutate(dummy_street = ifelse(lengths(touching_street) == 0, 0, 1)) %>%
#   filter(sum(dummy_street) >= 2)
#
# parcel_id <- unique(cross_corner$group_id.x)
#
# corner_parcel <- parcels %>%
#   mutate(id = 1:nrow(parcels)) %>%
#   filter(!id %in% long_unit_id$group_id) %>%
#   filter(id %in% parcel_id)
# # filter(pina == "10", pinb == "112", pinsa == "25")
#
# parcels <- parcels %>%
#   mutate(is_corner_cross = ifelse(id %in% parcel_id, 1, 0))


# visualization & validation

# parcel_fil <- parcels %>%
#   mutate(row = row_number()) %>%
#   filter(pina == "10", pinb == "112", pinsa == "25")

# cross %>%
#   st_as_sf() %>%
#   mutate(is_neighboring = cross_int) %>%
#   filter(group_id %in% parcel_fil$row) %>%
#   select(group_id, is_neighboring) %>%
#   ggplot() +
#   geom_sf(data = st_geometry(parcel_fil)) +
#   geom_sf(aes(color = is_neighboring)) +
#   #geom_sf(data = st_geometry(parcels)) +
#   geom_sf(data = st_geometry(corner_parcel), fill = 'blue') +
#   theme_void()

ggplot() + 
  geom_sf(data = st_geometry(clip_parcel[[1]])) +
  geom_sf(data = st_geometry(parcel[1]), fill = 'blue') + 
  geom_sf(data = st_as_sf(clip_network_test[[1]], "edges"), col = "grey50") +
  geom_sf(data = st_as_sf(clip_network_test[[1]], "nodes")) +
  theme_void()



##################
# parcels & streetnetwork
# targeted parcel & clip parcel & clip network
