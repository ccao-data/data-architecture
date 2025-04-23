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
    FROM awsdatacatalog.proximity.dist_pin_to_vacant_land
)
AND (p.pin10, p.year) NOT IN (
    SELECT DISTINCT pin10, year
    FROM awsdatacatalog.proximity.dist_pin_to_vacant_land
)
")
df <- noctua::dbGetQuery(AWS_ATHENA_CONN_NOCTUA, sql_query)

vacant_lands <- glue::glue("
    SELECT
        parcel.pin10,
        parcel.lon,
        parcel.lat,
        ST_ASBINARY(ST_POINT(parcel.x_3435, parcel.y_3435)) AS geometry_3435,
        parcel.year
    FROM spatial.parcel AS parcel
    INNER JOIN iasworld.pardat AS pardat
        ON parcel.pin10 = SUBSTR(pardat.parid, 1, 10)
        AND parcel.year = pardat.taxyr
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND pardat.class = '100'
")


vacant_lands <- noctua::dbGetQuery(AWS_ATHENA_CONN_NOCTUA, vacant_lands)

vacant_lands_2024 <- vacant_lands %>%
  filter(year == 2024)

df_2024 <- df %>%
  filter(year == 2024)

leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%

  # Plot the specific pin in blue
  addCircleMarkers(
    data = df_2024,
    ~lon, ~lat,
    color = "blue",
    fillOpacity = 0.8,
    radius = 6,
    label = ~"Selected PIN",
    group = "Selected PIN"
  ) %>%

  addCircleMarkers(
    data = vacant_lands_2024,
    ~lon, ~lat,
    color = "red",
    fillOpacity = 0.5,
    radius = 4,
    label = ~glue("PIN: {pin10}"),
    group = "New Constructions"
  ) %>%

  addLayersControl(
    overlayGroups = c("Selected PIN", "New Constructions"),
    options = layersControlOptions(collapsed = FALSE)
  )



vacant_lands_df <- as_data_frame(vacant_lands)

