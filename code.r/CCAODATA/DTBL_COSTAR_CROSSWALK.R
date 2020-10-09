library(odbc)
library(DBI)
library(magrittr)
library(tidyverse)
library(dplyr)
library(sf)
library(ccao)

CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))



# ---------------------- Way 1, match with parcel Pin ID ----------------------
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

# generate report on the jointed data
# Fetch the Lat and Long columns from costarsnapshot
query_gis <- "select ID, costar_latitude, costar_longitude from COSTARSNAPSHOTS where ID is not null and
             costar_latitude is not null and costar_longitude is not null and costar_tax_year = '2018' "
# create sf spatial points
df_costar_gis <- dbGetQuery(CCAODATA, query_gis) %>%
  # Convert gis data from costar into spatial object can be joined to shape file
  st_as_sf(coords = c("costar_longitude", "costar_latitude"), crs = 4326) %>% st_transform(crs = 3435)
head(df_costar_gis)

# Historical_Parcel_2018.GeoJSON if transofmr to crs = 3435, will return "MULTIPOLYGON EMPTY" since 
# It means the result of the intersection is an empty geometry (the points don't intersect). 
# It's still a geometry collection but it just don't have any geometry object in it.
# gis_shapes <- st_read(dsn = "//fileserver/ocommon/CCAODATA/data/spatial/Historical_Parcels__2019.GeoJSON") %>% 
df_ccao_gis <- st_read(dsn = "//fileserver/ocommon/CCAODATA/data/spatial/Historical_Parcels__2018.GeoJSON") %>%
  rename(PIN = PIN10) %>%
  filter(!is.na(as.numeric(PIN)) & nchar(PIN) %in% c(10, 14)) %>%
  mutate(PIN = pin_format_pretty(PIN))  %>%
  # st_as_sf( crs = 3435) %>%  
  st_transform(crs = 3435)

head(df_ccao_gis)

joined <- st_join(df_ccao_gis, df_costar_gis)
head(joined)
unique(joined$ID)

# ---------------------- Way 3, match by address ---------------------




















