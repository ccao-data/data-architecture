# loading the libraries
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
library(profvis)
library(furrr)



# NEED TO DO Jefferson Northwood Palinite Ridgeville South West Worth

township <- "Calumet"

bbox <- ccao::town_shp %>%
  filter(township_name == township) %>%
  st_bbox()

# Code to minimize size if necessary
# bbox <- st_bbox(c(xmin = -87.62, ymin = 41.645, xmax = -87.625, ymax = 41.65))


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
network <- as_sfnetwork(town_osm_center, directed = FALSE) %>%
  activate(edges) %>%
  arrange(edge_length()) %>%
  filter(!edge_is_multiple()) %>%
  filter(!edge_is_loop()) %>%
  st_transform(3435) %>%
  convert(to_spatial_simple) %>%
  # convert(to_spatial_smooth) %>%
  convert(to_spatial_subdivision) %>%
  activate(nodes) %>%
  mutate(degree = centrality_degree())

# Parcel data
parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>% 
  mutate(id = row_number())

# Code to minimize size if necessary
# parcels <- parcels %>%
#  filter(latitude >= 41.645 & latitude <= 41.65) %>%
#  filter(longitude <= -87.625 & longitude >= -87.62) 
  


# Create a nested list for target parcel, neighbor parcel, neighbor network
## Find the neighbor parcel clip
parcels <- parcels %>%
  st_transform(3435) %>%
  mutate(buffer_area = st_buffer(geometry, dist = units::set_units(100, "m"))) %>%
  mutate(clip_parcel = st_intersects(., buffer_area)) %>%
  st_transform(4326)

clip_parcel <- map(parcels$clip_parcel, function(x) {
  st_transform(parcels$geometry[x], 4326)
}) # change back to full file


# Find the neighbor network clip
clip_network <- c()
for (i in 1:nrow(parcels)) {
  clip_network[[i]] <- network %>%
    activate("edges") %>%
    st_filter(parcels$buffer_area[i], .pred = st_overlaps) %>%
    activate("nodes") %>%
    filter(!node_is_isolated()) %>%
    activate("edges") %>%
    st_as_sf() %>%
    st_transform(4326)
}

parcel <- parcels$geometry

sf_use_s2(FALSE)


### NOTHING CHANGED ABOVE
# 
# 
# plot(parcels[1])
# 
# shapefile <- st_read("your_shapefile.shp")
# 
# 
# save_objects_within_distance <- function(object, distance, output_dir) {
#   # Calculate distances between the object and all other objects
#   distances <- st_distance(object, shapefile)
#   
#   # Filter objects within the specified distance
#   nearby_objects <- shapefile[distances <= distance, ]
#   
#   # Save the filtered object to a separate file
#   output_file <- paste0(output_dir, "/object_", object$ID, ".shp")
#   st_write(nearby_objects, output_file)
# }
# 


#### NOTHING CHANGED BELOW

# Step 1: Create the minimum rectangle
min_rectangle <- st_minimum_rotated_rectangle(parcel)
min_rectangle_line <- st_segments(min_rectangle) %>%
  st_transform(crs = 4326)

# Step 2: Draw the cross
## find out the bearing angle
rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>%
  activate(edges) %>%
  mutate(bearing = edge_azimuth()) %>%
  mutate(bearing = units::set_units(bearing, "degree")) %>%
  # mutate(group_id = rep(1:nrow(parcels), each = 4)) %>%
  mutate(id = row_number()) %>%
  mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, "degree"))) %>%
  st_as_sf()

## find out distance
rectangle_network <- rectangle_network %>%
  mutate(length = st_length(x) + units::set_units(10, "m"))

## find out starting point
centriod <- st_centroid(parcel) %>% st_as_sf()
centriod <- centriod %>%
  slice(rep(1:n(), each = 4)) %>%
  st_transform(crs = 4326) %>%
  st_coordinates()

## draw the cross
dest <- destPoint(p = centriod, b = rectangle_network$corrected_bearing, d = rectangle_network$length)

cross <- cbind(centriod, dest) %>% as.data.frame()



# DRAW LINE FUNCTION
draw_line <- function(r) {
  st_linestring(t(matrix(unlist(r), 2, 2)))
}





# CROSS GEOM FUNCTION

cross$geom <- st_sfc(sapply(1:nrow(cross),
                            function(i) {
                              draw_line(cross[i, ])
                            },
                            simplify = FALSE
))

cross <- cross %>%
  st_set_geometry("geom") %>%
  st_set_crs(4326) %>%
  mutate(
    length = st_length(geom),
    aspect_ratio = as.numeric(lag(length) / length),
    id = rep(1:(nrow(.) / 4), each = 4)
  )

cross_lst <- cross %>%
  group_by(id) %>%
  group_split()


# Create the function 
crossing <- function(parcel, cross, network, neighbor_parcel, rectangle_network) {
  
  # Find out all the units touched by the cross
  touching_unit <- suppressWarnings(suppressMessages(st_intersects(cross$geom, neighbor_parcel)))
  
  # Step 3: Find out all the neighbor units for a parcel
  neighbor_unit <- suppressWarnings(suppressMessages(st_touches(parcel, neighbor_parcel)))
  neighbor_unit <- rep(neighbor_unit, each = 4)
  
  # Step 4: Find out the intersection of {cross_touching_unit & neighbor_unit}
  neighbor_and_touching <- list()
  for (i in 1:4) {
    neighbor_and_touching[[i]] <- intersect(touching_unit[[i]], neighbor_unit[[i]])
  }
  
  # Step 5: Remove the cross segments in the intersection of {cross_touching_unit & neighbor_unit}
  
  cross_int <- suppressMessages(imap_lgl(neighbor_and_touching, function(x, i) {
    any(map_lgl(x, function(y) {
      as.logical(st_intersects(cross$geom[i], neighbor_parcel[y]))
    }))
  }))
  
  cross_filter <- cross %>%
    mutate(is_neighboring = cross_int) %>%
    mutate(id = 1:4) %>%
    filter(!is_neighboring)
  
  # Step 6: Calculate the angle of cross segments
  angle_diff <- function(theta1, theta2) {
    theta <- abs(theta1 - theta2) %% 360
    return(ifelse(theta > 180, 360 - theta, theta))
  }
  
  cross_corner <- rectangle_network %>%
    mutate(bearing = as.numeric(bearing)) %>%
    filter(id %in% cross_filter$id) %>%
    mutate(diff_degree = angle_diff(bearing, lag(bearing))) %>%
    filter(diff_degree >= 85 & diff_degree <= 95, na.rm = TRUE)
  
  touching_unit_street <- suppressWarnings(suppressMessages(st_intersects(cross$geom, network)))
  touching_street_number <- sum(lengths(touching_unit_street))
  
  cross_corner_number <- nrow(cross_corner)
  
  aspect_ratio <- max(cross$aspect_ratio, na.rm = TRUE)
  
  
  if (all(touching_street_number >= 2) & all(cross_corner_number >= 1) & all(aspect_ratio < 30)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}



# Apply the above function to all parcels
num_threads <- parallel::detectCores(logical = FALSE)
plan(multisession, workers = 2)

# ## PARALLEL
# future_pmap_lgl(
#   list(parcel, cross_lst, clip_network, clip_parcel),
#   function(p, c, cl, cp) {
#     crossing(p, c, cl, cp, rectangle_network)
#   },
#   .options = furrr_options(seed = NULL), 
#   .progress = TRUE
# )


## SEQUENTIAL
corner_indicator <- c()

for (i in 1:98) {
  cross_idx <- (i - (i %% 4) + 1):(i - (i %% 4) + 4)
  corner_indicator[[i]] <- crossing(parcel[i], cross_lst[[i]], clip_network[[i]], clip_parcel[[i]], rectangle_network)
}


for (i in 1:98) {
  cross_idx <- (i - (i %% 4) + 1):(i - (i %% 4) + 4)
  
  # Extract and store the components
  parcel_list[[i]] <- parcel[i]
  cross_list[[i]] <- cross[cross_idx, ]
  clip_network_list[[i]] <- clip_network[[i]]
  clip_parcel_list[[i]] <- clip_parcel[[i]]
  rectangle_network_vector[[i]] <- rectangle_network
}




corner_parcel_calumet <- parcels %>%
  mutate(corner_indicator = unlist(corner_indicator)) %>%
  filter(corner_indicator == TRUE)


ggplot() +
  geom_sf(data = st_geometry(parcels)) +
  geom_sf(data = st_geometry(corner_parcel_calumet), fill = "blue") +
  geom_sf(data = st_as_sf(network, 'edges'), col = 'green') +
  theme_void()

# write_csv(corner_parcel_calumet, 'corner_calumet_1.csv')






