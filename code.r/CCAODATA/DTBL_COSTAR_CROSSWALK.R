library(odbc)
library(DBI)
library(magrittr)
library(tidyverse)
library(dplyr)
library(sf)
library(ccao)

CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))



# ---------------------- Way 2, match with parcel Pin ID ----------------------
# SQL PULLS ----

# Fetch the Costar Parcel pins from costarsnapshot
query_PIN_costar <- "select costar_parcel_number_1min, costar_parcel_number_2max from COSTARSNAPSHOTS 
                       where costar_parcel_number_1min is not null and costar_parcel_number_2max is not null and costar_tax_year = '2018'"
query_PIN_ccao <- "select PIN from AS_HEADT where TAX_YEAR = 2018"

# clean the data before deep pre-processing
df_costar_parcelPIN <- dbGetQuery(CCAODATA, query_PIN_costar) 
df_ccao_parcelPIN <- dbGetQuery(CCAODATA, query_PIN_ccao) %>% mutate(PIN = pin_format_pretty(PIN))

head(df_costar_parcelPIN)
head(df_ccao_parcelPIN)
# pending on this method, pretty complicated


# ------------------ Way 2, match with Latitude/Longevity -----------------
# SQL PULLS ----

# Fetch the Lat and Long columns from costarsnapshot
query_gis <- "select ID, costar_latitude, costar_longitude from COSTARSNAPSHOTS where ID is not null and
             costar_latitude is not null and costar_longitude is not null and costar_tax_year = '2018' "
# create sf spatial points
df_costar_gis <- dbGetQuery(CCAODATA, query_gis) %>%
  rename(latitude = costar_latitude,  longitude = costar_longitude ) %>% 
  # Convert gis data from costar into spatial object can be joined to shape file
  st_as_sf(x = .,  coords = c('longitude', 'latitude'),
           crs = "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")


head(df_costar_gis)


# Pull data from shape file in O drive

poly_gis_shapes <- st_read(dsn = "//fileserver/ocommon/CCAODATA/data/spatial/Historical_Parcels__2018.GeoJSON") %>%
  # format PINs
  rename(PIN = PIN10) %>%
  filter(!is.na(as.numeric(PIN)) & nchar(PIN) %in% c(10, 14)) %>%
  mutate(PIN = pin_format_pretty(PIN)) %>%
  # some PINs have multiple polygons. we'll choose the largest polygon and discard the others to keep our data unique by PIN
  group_by(PIN) %>%
  filter(ShapeSTAre == max(ShapeSTAre)) %>% 
  ungroup() %>% st_as_sf(.)

# st_as_sf(poly_gis_shapes)
# head(poly_gis_shapes)

# Join the df_costar_gis with gis_shapes by their spatial points

st_crs(df_costar_gis) <- st_crs(poly_gis_shapes) 

# join the two table (point to Multipolygon)
# joined <- st_join(st_as_sf(poly_gis_shapes), df_costar_gis)

joined <- st_contains(poly_gis_shapes,df_costar_gis, sparse = F)
head(joined)
unique(joined$PIN10)
unique(joined$ID)

# generate report on the jointed data

# ---------------------- Way 3, match by address ---------------------




















