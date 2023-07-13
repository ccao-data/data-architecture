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
# library(terra)
# library(raster)

# Jefferson Lake Norwood South West Worth

# parcelsfull <- parcels



townshipa <- "Worth"
township <- "Worth"

bbox <- ccao::town_shp %>%
  filter(township_name == townshipa) %>%
  st_bbox()





# Define the coordinates
xmin <- -87.79912
ymin <- 41.64626
xmax <- -87.67970
ymax <- 41.73552

# Calculate the width and height of the area
width <- xmax - xmin
height <- ymax - ymin

# Divide the width and height into four equal parts
sub_width <- width / 2
sub_height <- height / 2

# Create four bounding boxes
bbox1 <- c(xmin, ymin, xmin + sub_width, ymin + sub_height)
bbox2 <- c(xmin + sub_width, ymin, xmax, ymin + sub_height)
bbox3 <- c(xmin, ymin + sub_height, xmin + sub_width, ymax)
bbox4 <- c(xmin + sub_width, ymin + sub_height, xmax, ymax)

# Print the bounding boxes
print(bbox1)
print(bbox2)
print(bbox3)
print(bbox4)










split_bbox <- function(bbox, n_x = 2, n_y = 2) {
  # Assert that bbox is a bounding box
  stopifnot(class(bbox) %in% "bbox")
  
  # Get the length of the x axis
  x_ext <- bbox["xmax"] - bbox["xmin"]
  # Get the length of the y axis
  y_ext <- bbox["ymax"] - bbox["ymin"]
  
  incr_x <- x_ext / n_x
  incr_y <- y_ext / n_y
  
  # Create a sequence of x and y coordinates to generate the xmins and ymins
  xmin <- seq(from = bbox["xmin"], to = bbox["xmax"], by = incr_x)
  ymin <- seq(from = bbox["ymin"], to = bbox["ymax"], by = incr_y)
  
  # Remove the last element of x and y to ensure that the
  # top right corner isnt create an xmin or ymin
  xmin <- xmin[1:length(xmin) - 1]
  ymin <- ymin[1:length(ymin) - 1]
  
  bbox_table <-
    expand_grid(xmin, ymin) %>%
    mutate(xmax = xmin + incr_x,
           ymax = ymin + incr_y)
  
  bounding_boxes <-
    transpose(bbox_table) %>% map( ~ .x %>%
                                     unlist %>% st_bbox(crs = st_crs(bbox)$epsg))
  
  return(bounding_boxes)
}

split_bbox(bbox, n_x = 5, n_y = 5)


# Street network data
osm_data <- function(type) {
  opq(bbox = bbox4) %>%
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



# Parcel data
parcels <- st_read(
  glue::glue(
    "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?PoliticalTownship=Town%20of%20{township}&$limit=1000000"
  )) %>%
  mutate(id = row_number())


# bbox_grid <- st_make_grid(bbox)
# plot(bbox_grid)
# individual_bboxes <- st_geometry(bbox_grid)
# 










# # Access the first individual bounding box
# first_bbox <- individual_bboxes[[1]]
# spatial_bbox <- st_bbox(first_bbox)
# spatial_bbox[1] <- spatial_bbox[1] -.001
# spatial_bbox[2] <- spatial_bbox[2] -.001
# spatial_bbox[3] <- spatial_bbox[3] +.001
# spatial_bbox[4] <- spatial_bbox[4] +.001


parcels <- parcels %>%
  filter(latitude >= bbox4[2] & latitude <= bbox4[4]) %>%
  filter(longitude <= bbox4[1] & longitude >= bbox4[3]) %>%
  st_transform(4326)


















dan_func_single_obs <- function(x, parcels_full, parcels_buffered, network) {

  stopifnot(
    st_crs(parcels_full) == st_crs(parcels_buffered),
    st_crs(parcels_buffered) == st_crs(network)
  )

  # Step 1: Create the minimum rectangle
  min_rectangle <- parcels_full[x, ] %>%
    st_minimum_rotated_rectangle() %>%
    select(geometry)
  
  min_rectangle_line <- st_segments(min_rectangle) %>%
    st_transform(crs = 4326)


  # Step 2: Draw the cross
  ## Find out the bearing angle
  rectangle_network <- as_sfnetwork(min_rectangle_line, directed = FALSE) %>%
    activate(edges) %>%
    mutate(bearing = edge_azimuth()) %>%
    mutate(bearing = units::set_units(bearing, "degree")) %>%
    mutate(id = row_number()) %>%
    mutate(corrected_bearing = ifelse(id %% 4 != 0, bearing, bearing + units::set_units(180, "degree"))) %>%
    st_as_sf()


  ## Step 3 Find out the distance
  rectangle_network <- rectangle_network %>%
    mutate(length = st_length(result) + units::set_units(10, "m"))

  ## Step 4 Find out the starting point
  centroid <- suppressWarnings({
    st_centroid(parcels_full[x, ]) %>%
      st_as_sf() %>%
      st_transform(4326) %>%
      select(geometry)
  })
  
  # 
  # 
  # centroid_transformed <- st_transform(centroid, crs = 3435)
  # buffer_distance <- 500
  # bbox_buffer <- st_buffer(centroid_transformed, dist = buffer_distance)
  # clipped_network <- suppressWarnings(suppressMessages(st_crop(network, bbox_buffer)))
  # 
  # 

    
  centroid <- centroid %>%
    slice(rep(1:n(), each = 4)) %>%
    st_transform(crs = 4326) %>%
    st_coordinates()


  ## Step 5 Draw the cross
  dest <- destPoint(p = centroid, b = rectangle_network$corrected_bearing, d = rectangle_network$length)

  cross <- cbind(centroid, dest) %>%
    as.data.frame()


  ## Step 6 Draw Line
  draw_line <- function(r) st_linestring(t(matrix(unlist(r), 2, 2)))


  ## Step 7 Cross Function
  cross$geometry <- st_sfc(sapply(1:nrow(cross), function(i) {
    draw_line(cross[i, ])
  }, simplify = FALSE))

  cross <- cross %>%
    st_set_geometry("geometry") %>%
    st_set_crs(4326) %>%
    mutate(
      length = st_length(geometry),
      aspect_ratio = as.numeric(lag(length) / length),
      id = rep(1:(nrow(.) / 4), each = 4)
    ) %>%
    st_transform(3435)

  # Find out all the units touched by the cross
  touching_unit <- suppressWarnings(suppressMessages(st_intersects(cross$geometry, parcels_full)))

  # Step 3: Find out all the neighbor units for a parcel
  neighbor_unit <-
    list(which(replace_na(as.logical(st_intersects(parcels_full, parcels_buffered[x, ])), FALSE)))
  neighbor_unit <- rep(neighbor_unit, each = 4)

  # Step 4: Find out the intersection of {cross_touching_unit & neighbor_unit}
  neighbor_and_touching <- lapply(1:4, function(i) setdiff(intersect(touching_unit[[i]], neighbor_unit[[i]]), x))

  # Step 5: Remove the cross segments in the intersection of {cross_touching_unit & neighbor_unit}
  # cross_int <- suppressMessages(imap_lgl(neighbor_and_touching, function(x, i) {

  cross_int <- replace_na(imap_lgl(neighbor_and_touching, function(y, i) {
     as.logical(st_intersects(cross$geometry[i], parcels_full[i, ]))
   }), FALSE)

  cross_int <- map_lgl(neighbor_and_touching, function(x) length(x) > 0)

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
    arrange(id) %>%
    mutate(
      diff_degree = angle_diff(bearing, lag(bearing)),
      diff_degree = replace_na(diff_degree, 0)
    ) %>%
    filter((diff_degree >= 85 & diff_degree <= 95), na.rm = TRUE) %>%
    st_set_geometry("result") %>%
    st_transform(3435)

  

  
  touching_unit_street <- suppressWarnings(suppressMessages(
    st_intersects(cross_filter$geometry, network)
  ))

  touching_street_number <- sum(lengths(touching_unit_street))

  cross_corner_number <- nrow(cross_corner)

  aspect_ratio <- max(cross$aspect_ratio, na.rm = TRUE)
  


  if (all(touching_street_number >= 2) & all(cross_corner_number >= 1) & all(aspect_ratio < 30)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

# Prepare inputs
parcels_buffered <- parcels %>%
  st_transform(3435) %>%
  st_buffer(dist = units::set_units(5, "m"))

network_trans  <- network %>%
  activate("edges") %>%
  activate("nodes") %>%
  filter(!node_is_isolated()) %>%
  activate("edges") %>%
  st_as_sf() %>%
  st_transform(3435)


# dan_func_single_obs(9, parcels %>% st_transform(3435), parcels_buffered, network_trans)

# 
 result <- numeric(nrow(parcels))
# 
for (x in 1:nrow(parcels)) {
   result[x] <- dan_func_single_obs(x, parcels %>% st_transform(3435), parcels_buffered, network_trans)
   cat("Iteration:", x, "/", nrow(parcels), "\n")  # Print iteration progress
}
 
 




#
final <- cbind(parcels, result)





 # Define the township and file name
 township <- "Worth4"
 file_name <- paste0(township, ".shp")

 # Specify the directory path
 directory <- "Base Data"

 # Create the complete file path
 output_file <- file.path(directory, file_name)

 # Create an sf object with your data
 sf_obj <- final  # Replace `your_sf_object` with your actual sf object

 sf_obj <- sf_obj %>%
   select(pin10, result, geometry)

 sf_obj <- sf_obj %>%
   subset(result == 1)

 # Save the complete shapefile
 sf::st_write(sf_obj, output_file, append = FALSE)







 # Define the township and file name
 file_name <- paste0(township, ".csv")

 # Specify the directory path
 directory <- "Base Data"


 # Create the complete file path
 output_file <- file.path(directory, file_name)

 st_write(sf_obj, output_file, driver = "CSV", append = TRUE)









 file_name <- paste0("Streets", township, ".shp")

 output_file <- file.path(directory, file_name)

 output_file <- file.path(directory, file_name)


 data <- network_trans[-1, ]  # Remove the first row containing metadata

 # Define the shapefile attributes
 attribute_fields <- c("osm_id", "name")  # Add the fields you want to include as attributes

 # Create a spatial data frame
 lines <- st_as_sf(data, coords = c("from", "to"), crs = 3435)  # Replace 3435 with the appropriate EPSG code for NAD83 / Illinois East (ftUS)

 # Assign attributes to the spatial data frame
 st_drop_geometry(lines)  # Drop the geometry temporarily to assign attributes
 for (field in attribute_fields) {
   lines[[field]] <- data[[field]]
 }

 lines <- lines %>%
   select(osm_id)





 if (file.exists(file_name)) {
   file.remove(file_name)
 }

 st_write(lines, output_file, append = FALSE)





 ggplot() +
   geom_sf(data = final, aes(fill = result)) +
   geom_sf(data = st_as_sf(network, 'edges'), col = 'green')







