# This script creates a crosswalk linking Costar IDs with CCAO PINs

# load necessary libraries
library(odbc)
library(DBI)
library(tidyverse)
library(dplyr)
library(sf)
library(ccao)
library(installr)

# connect to SQL server
CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

# SQL PULLS ----

# define costar data as Costar ID, lat, long, year columns from COSTARSNAPSHOTS
costar_query <- "
select ID, costar_latitude, costar_longitude, convert(int, costar_tax_year) as [TAX_YEAR]
from COSTARSNAPSHOTS

where ID is not null and
costar_latitude is not null and costar_longitude is not null"

# join Costar data and parcel shapefile
joined <- st_join(

  # gather costar data
  dbGetQuery(CCAODATA, costar_query) %>%

    # convert costar data into into a spatial object can be joined to shape file
    st_as_sf(coords = c("costar_longitude", "costar_latitude"), crs = 4326) %>%
    st_transform(crs = 3435),

  # read and process parcel shapefile
  read_sf(dsn = "O:/CCAODATA/data/spatial/Historical_Parcels__2019/Historical_Parcels__2019.shp",
          layer = 'Historical_Parcels__2019') %>%

    # clean columns
    rename(PIN = PIN10) %>%
    filter(!is.na(as.numeric(PIN)) & nchar(PIN) %in% c(10, 14)) %>%
    mutate(PIN = pin_format_pretty(PIN))  %>%

    # some PINs have multiple polygons. we'll choose the largest polygon and discard the others to keep our data unique by PIN
    group_by(PIN) %>%
    filter(SHAPE_Area == max(SHAPE_Area)) %>%
    ungroup() %>%

    # it seems there are some duplicate rows in the shape file itself
    filter(!duplicated(PIN))

) %>%

  # filter out bad joins/bad Costar data
  filter(!is.na(PIN) & !is.na(TAX_YEAR)) %>%

  # convert to non-geographic object
  data.frame() %>%

  # remove unnecessary data
  select(c(ID, PIN, TAX_YEAR)) %>%

  #format PIN so it can be joined with other SQL tables
  mutate(PIN = paste0(gsub("-", "", PIN), "0000"))

# INTEGRITY CHECKS ----

# check for duplicated records after join
print(
  paste0(
    nrow(joined) - nrow(joined %>% distinct()), " non-distinct rows in DTBL_COSTAR_CROSSWALK"
    )
  )

# match rate
print(
  paste0(
    "Total Match Rate: ", label_percent()(nrow(joined) / dbGetQuery(CCAODATA, "SELECT COUNT(ID) FROM COSTARSNAPSHOTS")[[1]])
    )
  )

# UPLOAD TO SQL SERVER ----

if (ask.user.yn.question("Are you certain you want to overwrite DTBL_COSTAR_CROSSWALK?") == TRUE) {

  # replace read credentials with write credentials
  CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATAW"))

  # overwrite table on server
  dbWriteTable(CCAODATA, "DTBL_COSTAR_CROSSWALK", joined, overwrite = TRUE, row.names = FALSE)

}