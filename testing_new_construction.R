library(glue)
library(leaflet)
library(noctua)
library(dplyr)
library(DBI)
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

sql_query <- glue::glue("
SELECT DISTINCT p.pin10, p.year, p.lat, p.lon
FROM awsdatacatalog.spatial.parcel p
WHERE p.year >= (
    SELECT MIN(year)
    FROM awsdatacatalog.proximity.dist_pin_to_new_construction
)
AND (p.pin10, p.year) NOT IN (
    SELECT DISTINCT pin10, year
    FROM awsdatacatalog.proximity.dist_pin_to_new_construction
)
")
df <- noctua::dbGetQuery(AWS_ATHENA_CONN_NOCTUA, sql_query)

new_constructions <- glue::glue("
  SELECT
    parcel.pin10,
    parcel.lat,
    parcel.lon,
    parcel.year,
    vw_card_res_char.char_yrblt
  FROM spatial.parcel AS parcel
  INNER JOIN default.vw_card_res_char AS vw_card_res_char
    ON parcel.pin10 = vw_card_res_char.pin10
    AND parcel.year = vw_card_res_char.year
    AND CAST(parcel.year AS INT) <= CAST(vw_card_res_char.char_yrblt AS INT) + 3
  WHERE parcel.year = '2017'
")

new_constructions <- noctua::dbGetQuery(AWS_ATHENA_CONN_NOCTUA, new_constructions)

test <- new_constructions %>%
  filter(pin10 == '1428319084')


leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%

  # Plot the specific pin in blue
  addCircleMarkers(
    data = df,
    ~lon, ~lat,
    color = "blue",
    fillOpacity = 0.8,
    radius = 6,
    label = ~"Selected PIN",
    group = "Selected PIN"
  ) %>%

  # Plot new constructions in red
  addCircleMarkers(
    data = test,
    ~lon, ~lat,
    color = "black",
    fillOpacity = 0.5,
    radius = 8,
    label = ~glue("PIN: {pin10}, YrBlt: {char_yrblt}"),
    group = "New Constructions"
  ) %>%

  addCircleMarkers(
    data = new_constructions,
    ~lon, ~lat,
    color = "red",
    fillOpacity = 0.5,
    radius = 4,
    label = ~glue("PIN: {pin10}, YrBlt: {char_yrblt}"),
    group = "New Constructions"
  ) %>%

  addLayersControl(
    overlayGroups = c("Selected PIN", "New Constructions"),
    options = layersControlOptions(collapsed = FALSE)
  )



library(glue)
library(leaflet)
library(noctua)
library(dplyr)
library(DBI)
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

sql_query <- glue::glue("
SELECT DISTINCT p.pin10, p.year, p.lat, p.lon
FROM awsdatacatalog.spatial.parcel p
WHERE p.year >= (
    SELECT MIN(year)
    FROM awsdatacatalog.proximity.dist_pin_to_new_construction
)
AND (p.pin10, p.year) NOT IN (
    SELECT DISTINCT pin10, year
    FROM awsdatacatalog.proximity.dist_pin_to_new_construction
)
")
df <- noctua::dbGetQuery(AWS_ATHENA_CONN_NOCTUA, sql_query)

new_constructions <- glue::glue("
  SELECT
    parcel.pin10,
    parcel.lat,
    parcel.lon,
    parcel.year,
    vw_card_res_char.char_yrblt
  FROM spatial.parcel AS parcel
  INNER JOIN default.vw_card_res_char AS vw_card_res_char
    ON parcel.pin10 = vw_card_res_char.pin10
    AND parcel.year = vw_card_res_char.year
    AND CAST(parcel.year AS INT) <= CAST(vw_card_res_char.char_yrblt AS INT) + 3
  WHERE parcel.year = '2017'
")

new_constructions <- noctua::dbGetQuery(AWS_ATHENA_CONN_NOCTUA, new_constructions)

test <- new_constructions %>%
  filter(pin10 == '1428319084')


leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%

  # Plot the specific pin in blue
  addCircleMarkers(
    data = df,
    ~lon, ~lat,
    color = "blue",
    fillOpacity = 0.8,
    radius = 6,
    label = ~"Selected PIN",
    group = "Selected PIN"
  ) %>%

  # Plot new constructions in red
  addCircleMarkers(
    data = test,
    ~lon, ~lat,
    color = "black",
    fillOpacity = 0.5,
    radius = 8,
    label = ~glue("PIN: {pin10}, YrBlt: {char_yrblt}"),
    group = "New Constructions"
  ) %>%

  addCircleMarkers(
    data = new_constructions,
    ~lon, ~lat,
    color = "red",
    fillOpacity = 0.5,
    radius = 4,
    label = ~glue("PIN: {pin10}, YrBlt: {char_yrblt}"),
    group = "New Constructions"
  ) %>%

  addLayersControl(
    overlayGroups = c("Selected PIN", "New Constructions"),
    options = layersControlOptions(collapsed = FALSE)
  )



new_constructions_df <- as_data_frame(new_constructions)



