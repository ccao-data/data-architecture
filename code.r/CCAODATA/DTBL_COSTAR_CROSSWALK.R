library(odbc)
library(DBI)
library(magrittr)
library(tidyverse)
library(dplyr)
library(sf)
library(ccao)
library(stringr)
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
query_gis <- "select ID, costar_latitude, costar_longitude, costar_tax_year from COSTARSNAPSHOTS where ID is not null and
             costar_latitude is not null and costar_longitude is not null  "
# create sf spatial points
df_costar_gis <- dbGetQuery(CCAODATA, query_gis) %>%
  # Convert gis data from costar into spatial object can be joined to shape file
  st_as_sf(coords = c("costar_longitude", "costar_latitude"), crs = 4326) %>% st_transform(crs = 3435)

# It's still a geometry collection but it just don't have any geometry object in it.
df_ccao_gis <- read_sf(dsn = "//fileserver/ocommon/CCAODATA/data/spatial/Historical_Parcels__2019/Historical_Parcels__2019.shp", layer = 'Historical_Parcels__2019') %>% 
          rename(PIN = PIN10) %>%
          filter(!is.na(as.numeric(PIN)) & nchar(PIN) %in% c(10, 14)) %>%
          mutate(PIN = pin_format_pretty(PIN))  %>%
          group_by(PIN) %>%
          filter(SHAPE_Area == max(SHAPE_Area)) %>%
          ungroup() %>%
          # it seems there are some duplicate rows in the shapefile itself
          filter(!duplicated(PIN))

# perform the join
joined <- st_join(df_costar_gis, df_ccao_gis )
head(joined)
head(joined)

## Check on Duplicates
unique(joined$costar_tax_year)
unique(joined$ID)



# ---------------------- Way 3, match by address ---------------------

query_ccao_address <- "select  PIN,  PROPERTY_ADDRESS, PROPERTY_APT_NO, PROPERTY_CITY, PROPERTY_ZIP 
                       from VW_PINGEO 
                      where most_recent = 2018 and PROPERTY_ADDRESS is not null and PIN is not null "
 
df_ccao_address <- dbGetQuery(CCAODATA, query_ccao_address)  %>% 
  # make sure the address string are capitalized and clean from white space
  mutate(PROPERTY_ADDRESS = str_trim(toupper(.$PROPERTY_ADDRESS)))
# head(df_ccao_address)
query_costar_address <- "select  ID,  
                        costar_building_address, costar_zip 
                        -- ,costar_parcel_number_1min, costar_parcel_number_2max  
                       from COSTARSNAPSHOTS 
                       where costar_tax_year = '2018' 
                       and ID is not null and costar_building_address is not null "
# 
df_costar_address <- dbGetQuery(CCAODATA, query_costar_address)
nrow(df_costar_address)
# head(df_costar_address)

# --- process and clean costar data

# function to clean zip code column
zipcode_process <- function(x) {
  if (nchar(x) <= 5){
    str_trim(x)
  }
  else{
    str_trim(paste0(substr(x, 1, 5), '-', substr(x, 6, nchar(x))))
  }
}

df_costar_address_ready <- df_costar_address %>% 
  #clean and extra white space, and capitalize the address string to match the CCAo data
  mutate(costar_building_address = str_trim(toupper(.$costar_building_address)))  %>% 
  mutate(costar_zip = sapply(costar_zip, zipcode_process))
# head(df_costar_address_ready)
# --- Join ccao and costar table 

# Joined_Address_Single <- df_ccao_address %>%
#                full_join(., df_costar_address_ready, by = c("PROPERTY_ADDRESS" = "costar_building_address") )

nrow(df_ccao_address)

# there was no obvious joined records ... need to look into more details tommr
left_join(df_ccao_address, df_costar_address_ready,  by=c(  "PROPERTY_ADDRESS" = "costar_building_address"  ) )

left_join(df_ccao_address, df_costar_address_ready, by=c(  "PROPERTY_ZIP" = "costar_zip"  )  )

left_join(df_ccao_address, df_costar_address_ready, by=c( "PROPERTY_ZIP" = "costar_zip",  "PROPERTY_ADDRESS" ="costar_building_address" )  )












