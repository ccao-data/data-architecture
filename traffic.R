library(sf)
library(DBI)
library(noctua)
library(dplyr)
library(leaflet)
library(ggplot2)

shapefile <- read_sf("etl/scripts-ccao-data-raw-us-east-1/spatial/traffic_data.shp")


filtered_data <- shapefile %>%
  filter(COUNTY_NAM == "COOK") %>%
  st_as_sf() %>%
  st_transform(crs = 4326) %>%
  mutate(AADT_STRIN = as.numeric(AADT_STRIN))

ggplot() +
  geom_sf(data = filtered_data, aes(color = AADT_STRIN), size = 1) + # Adjust size as needed
  scale_color_viridis_c(option = "plasma", name = "AADT_STRIN") +  # Use viridis color scale for better visualization
  labs(
    title = "Map of AADT_STRIN in Cook County",
    subtitle = "Visualizing Traffic Data on Cook County Roads",
    x = "Longitude",
    y = "Latitude"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5)
  )

filtered_data <- st_zm(filtered_data)


pal <- colorNumeric(palette = "viridis", domain = filtered_data$AADT_STRIN)

leaflet(filtered_data) %>%
  addProviderTiles("CartoDB.Positron") %>%  # Add a base map layer
  addPolylines(
    color = ~pal(AADT_STRIN),  # Use the color palette based on AADT_STRIN
    weight = 2,                # Adjust line thickness
    opacity = 0.7,             # Adjust line transparency
    popup = ~paste("AADT_STRIN:", AADT_STRIN)  # Add popups to show AADT_STRIN values
  ) %>%
  addLegend(
    pal = pal,
    values = ~AADT_STRIN,
    opacity = 0.7,
    title = "AADT_STRIN",
    position = "bottomright"
  ) %>%
  setView(lng = mean(st_coordinates(filtered_data)[, 1]),
          lat = mean(st_coordinates(filtered_data)[, 2]),
          zoom = 10)  # Adjust zoom level and map center

con <- dbConnect(noctua::athena())

# Assuming the original CRS is EPSG:3857 (Web Mercator), adjust this if necessary
secondary_roads <- dbGetQuery(con, 'SELECT * FROM "spatial"."secondary_road" WHERE CAST(year AS INTEGER) = 2023') %>%
  st_as_sf() %>%
  st_set_crs(3435)


# Leaflet map combining both filtered_data and secondary_roads
  # Leaflet map combining both filtered_data and secondary_roads
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%  # Base map layer
  addPolylines(
    data = filtered_data,
    color = ~pal(AADT_STRIN),  # Color based on AADT_STRIN
    weight = 2,                # Adjust line thickness
    opacity = 0.7,             # Adjust transparency
    popup = ~paste("AADT_STRIN:", AADT_STRIN)  # Popup for AADT_STRIN
  ) %>%
  addPolylines(
    data = secondary_roads,
    color = "blue",  # Color for secondary roads
    weight = 1,      # Adjust line thickness for secondary roads
    opacity = 0.6,   # Adjust transparency for secondary roads
    popup = ~paste("Road Name:", name)  # Add popups for secondary roads
  ) %>%
  addLegend(
    pal = pal,
    values = filtered_data$AADT_STRIN,
    opacity = 0.7,
    title = "AADT_STRIN",
    position = "bottomright"
  ) %>%
  setView(
    lng = mean(st_coordinates(filtered_data)[, 1]),
    lat = mean(st_coordinates(filtered_data)[, 2]),
    zoom = 10  # Adjust zoom level
  )


  # Step 2: Buffer the geometries by 50 feet (around filtered_data)
  filtered_data_buffer <- st_buffer(filtered_data, dist = 50)

  # Step 3: Spatial join to find intersections within 50 feet
  joined_data <- st_join(secondary_roads, filtered_data_buffer, join = st_intersects)

  # Step 4: Optionally, filter for rows where intersections occurred
  joined_data_within_50ft <- joined_data %>%
    filter(!is.na(AADT_STRIN))  # AADT_STRIN is from filtered_data, so this filters where the join occurred


