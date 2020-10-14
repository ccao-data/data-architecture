library(odbc)
library(DBI)
library(magrittr)
library(tidyverse)
library(dplyr)
library(sf)
library(ccao)
library(stringr)
CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))
# ------------------ Match with Latitude/Longevity into Parcel Polygon -----------------
# SQL PULLS ----

# generate report on the jointed data
# Fetch the ID, Lat and Long, Year columns from costarsnapshot
query_gis <- "select ID, costar_latitude, costar_longitude, costar_tax_year from COSTARSNAPSHOTS where ID is not null and
             costar_latitude is not null and costar_longitude is not null  "
# process the data into sf spatial points
df_costar_gis <- dbGetQuery(CCAODATA, query_gis) %>%
  # Convert gis data from costar into spatial object can be joined to shape file
  st_as_sf(coords = c("costar_longitude", "costar_latitude"), crs = 4326) %>% st_transform(crs = 3435)

# Read and porcess CCAO GIS data
# df_ccao_gis <- read_sf(dsn = "O:/CCAODATA/data/spatial/Historical_Parcels__2019/Historical_Parcels__2019.shp", layer = 'Historical_Parcels__2019') %>% 
  df_ccao_gis <- read_sf(dsn = "//fileserver/ocommon/CCAODATA/data/spatial/Historical_Parcels__2019/Historical_Parcels__2019.shp", layer = 'Historical_Parcels__2019') %>% 
  
          rename(PIN = PIN10) %>%
          filter(!is.na(as.numeric(PIN)) & nchar(PIN) %in% c(10, 14)) %>%
          mutate(PIN = pin_format_pretty(PIN))  %>%
          group_by(PIN) %>%
          filter(SHAPE_Area == max(SHAPE_Area)) %>%
          ungroup() %>%
          # it seems there are some duplicate rows in the shape file itself
          filter(!duplicated(PIN))

# perform the join
joined <- st_join(df_costar_gis, df_ccao_gis ) %>% 
  filter(!is.na(PIN) & !is.na(costar_tax_year)) %>% 
  select(c(ID, PIN, costar_tax_year))
# head(joined)


# check duplicated records after join
joined %>% filter(costar_tax_year== "2018" | costar_tax_year == "2017" | costar_tax_year == "2016") %>% 
  group_by(ID,PIN, costar_tax_year) %>% summarise(Count = n()) %>% filter(Count > 1)

# Integrity Check on the match rate + count

costar_rowcnt <- nrow(df_costar_gis)
join_rowcnt <- nrow(joined)
# match rate
match_rate <- join_rowcnt/costar_rowcnt
print(paste0("Total Match Count: ",join_rowcnt))
print(paste0("Match Rate: ",round(match_rate, 2)))





