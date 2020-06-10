# This script extracts XY coordinates for PINs from a Cook COunty parcels parcelsfile.

# ingest necessary functions for overwriting DTBL_PINLOCATIONS
source(paste0("C:/Users/", Sys.info()[['user']],"/Documents/ccao_utility/code.r/99_utility_2.r"))
database <- 1

# load needed packages
library(sf)
library(dplyr)
library(DBI)
library(odbc)
library(safer)
library(installr)

# spatial data files ----
directories <- list("parcels" = c("Historical_Parcels__2019/Historical_Parcels__2019.shp", "Historical_Parcels__2019"),
                    "tracts" = "tl_2018_17_tract.GeoJSON",
                    "pumas" = c("ipums_puma_2010/ipums_puma_2010.shp", "ipums_puma_2010"),
                    "leyden_ohare" = c("Half_Mile_Contour_Buffer/Half_Mile_Contour_Buffer.shp", "Half_Mile_Contour_Buffer"),
                    "floodplains" = c("NFHL_17_20190515.gdb", "S_Fld_Haz_Ar"),
                    "roads" = c("Streets-shp/Streets.shp", "Streets"),
                    "wards" = "wards.GeoJSON",
                    "commissioner" = "commissioner.GeoJSON",
                    "repres" = "repres_dict.GeoJSON",
                    "senate" = "senate_dict.GeoJSON",
                    "tif" = "tif_dict.GeoJSON",
                    "municipalities" = "tl_2013_17_place.GeoJSON",
                    "ssas" = "SSAs.GeoJSON")

# specific columns to ingest for each file ----
columns <- list("parcels" = c("Name", "PIN10", "PINB", "SHAPE_Area", "geometry"),
                "tracts" = c("TRACTCE", "geometry", "GEOID"),
                "pumas" = c("PUMA", "geometry"),
                "leyden_ohare" = c("AIRPORT", "geometry" ),
                "floodplains" = c("SFHA_TF", "SHAPE"),
                "roads" = c("MAJORROAD", "geometry"),
                "wards" = c("ward", "geometry" ),
                "commissioner" = c("district", "geometry"),
                "repres" = c("district_1", "geometry"),
                "senate" = c("senatedist", "geometry"),
                "tif" = c("agencynum", "geometry"),
                "municipalities" = c("PLACEFP", "NAME", "geometry"),
                "ssas" = c("name", "ref_no", "geometry"))

# ingest spatial data ----
spatial_data <- list()

for (i in names(directories)) {

  if (length(directories[[i]]) == 1) {

    spatial_data[[i]] <- read_sf(dsn = paste0("O:/CCAODATA/data/spatial/", directories[[i]])) %>%
      select(columns[[i]])

  } else {

    spatial_data[[i]] <- read_sf(dsn = paste0("O:/CCAODATA/data/spatial/", directories[[i]][1]), layer = directories[[i]][2]) %>%
      select(columns[[i]])

  }

}

# clean, prepare for joins ----

# buffer roads
spatial_data[["roads100"]] <- st_buffer(spatial_data[["roads"]], 100)
spatial_data[["roads300"]] <- st_buffer(spatial_data[["roads"]], 300)
spatial_data[["roads"]] <- NULL

# rename columns/subset spatial data
spatial_data[["leyden_ohare"]] <- rename(spatial_data[["leyden_ohare"]], "ohare_noise" = "AIRPORT")
spatial_data[["parcels"]] <- rename(spatial_data[["parcels"]], "PIN" = "Name")
spatial_data[["municipalities"]] <- rename(spatial_data[["municipalities"]],
                                           "municipality" = "NAME", "FIPS" = "PLACEFP")
spatial_data[["ssas"]] <- rename(spatial_data[["ssas"]], "ssa_name" = "name", "ssa_no" = "ref_no")
spatial_data[["commissioner"]] <- rename(spatial_data[["commissioner"]], "commissioner_dist" = "district")
spatial_data[["repres"]] <- rename(spatial_data[["repres"]], "reps_dist" = "district_1")
spatial_data[["senate"]] <- rename(spatial_data[["senate"]], "senate_dist" = "senatedist")
spatial_data[["tif"]] <- rename(spatial_data[["tif"]], "tif_agencynum" = "agencynum")

spatial_data[["floodplains"]] <- spatial_data[["floodplains"]] %>%
  filter(SFHA_TF == "T") %>%
  rename(., "floodplain" = "SFHA_TF")

spatial_data[["roads100"]] <- spatial_data[["roads100"]] %>%
  filter(MAJORROAD == "YES") %>%
  rename(., "withinmr100" = "MAJORROAD")

spatial_data[["roads300"]] <- spatial_data[["roads300"]] %>%
  filter(MAJORROAD == "YES") %>%
  rename(., "withinmr300" = "MAJORROAD")

# clean some columns
spatial_data[["ssas"]]$ssa_no <- gsub("SSA#| ", "", spatial_data[["ssas"]]$ssa_no)

# parcels shapefile can be a little wonky when it's first ingested
spatial_data[["parcels"]] <- st_make_valid(spatial_data[["parcels"]])

# if a PIN is 10 digits long, or if it isn't 14 digits long but the first 10 digits match PIN10, take the first 10 digits and add '0000'
spatial_data[["parcels"]]$PIN <- ifelse(nchar(spatial_data[["parcels"]]$PIN) == 10 |
                                          (nchar(spatial_data[["parcels"]]$PIN) != 14 &
                                             substr(spatial_data[["parcels"]]$PIN, 1, 10) == spatial_data[["parcels"]]$PIN10),
                                        paste0(substr(spatial_data[["parcels"]]$PIN, 1, 10), "0000"),
                                        spatial_data[["parcels"]]$PIN)

# record parcels where PIN10 does not match PIN and create observations for both "PIN" and "PIN10" values
problems <- spatial_data[["parcels"]] %>% filter(substr(PIN, 1, 10) != PIN10 | nchar(PIN) != 14)

write.csv(data.frame(problems) %>% select(-"geometry"),
          file = "O:/CCAODATA/data/bad_data/parcel_problems.csv", row.names = FALSE, na = "")

# remove those observations from the parcels data set
spatial_data[["parcels"]] <- spatial_data[["parcels"]] %>% filter(substr(PIN, 1, 10) == PIN10 & nchar(PIN) == 14)

# only keep problem observations with usable data
problems <- problems %>% filter(nchar(problems$PIN) == 14 & nchar(problems$PIN10) == 10)

# create an observation for both PIN10 and PIN if they don't match, make sure there are no duplicates
PINs <- problems %>% filter(nchar(PIN) == 14) %>% mutate("PIN10" = paste0(substr(PIN, 1, 10)))
pin10s <- problems %>% filter(nchar(PIN10) == 10) %>% mutate("PIN" = paste0(PIN10, "0000"))
problems <- rbind(PINs, pin10s) %>% filter(!duplicated(PIN))
spatial_data[["parcels"]] <- rbind(spatial_data[["parcels"]], problems)
rm(PINs, pin10s, problems)

# identify PINs with "999" values
spatial_data[["parcels"]]$PIN999 <- ifelse(spatial_data[["parcels"]]$PINB == 999, 1, 0)

# identify PINs that are points
spatial_data[["parcels"]]$point_parcel <- ifelse(spatial_data[["parcels"]]$SHAPE_Area < 0.5, 1, 0)

# identify pins with multiple geographies
spatial_data[["parcels"]]$PIN10 <- substr(spatial_data[["parcels"]]$PIN, 1, 10)
spatial_data[["parcels"]]$multiple_geographies <-
  ifelse((duplicated(spatial_data[["parcels"]]$PIN10) | duplicated(spatial_data[["parcels"]]$PIN10,
                                                                   fromLast = TRUE)) == TRUE, 1, 0)
spatial_data[["parcels"]]$primary_polygon <- ifelse(duplicated(spatial_data[["parcels"]]$PIN10) == FALSE, 1, 0)
spatial_data[["parcels"]]$PIN10 <- NULL

# analyze parcels as centroids
spatial_data[["parcels"]]$geometry <- st_centroid(spatial_data[["parcels"]]$geometry)

# set shapefiles to same CRS ----
for (i in names(spatial_data)) {

  spatial_data[[i]] <- st_transform(spatial_data[[i]], crs = 3435)

}

# work with a single data frame for joined data
output <- spatial_data[["parcels"]]

# Identify which pins are in a 100ft or 101 to 300ft buffer around major roads ---
join <- st_intersection(output, spatial_data[["roads100"]])
output$withinmr100 <- ifelse(output$PIN %in% join$PIN, 1, 0)

join <- st_intersection(output, spatial_data[["roads300"]])
output$withinmr300 <- ifelse(output$PIN %in% join$PIN, 1, 0)
rm(join)

# create a way to dedupe after possible duplication issues during joins
output$id <- 1:nrow(output)

# join/intersect parcels to other shapefiles ----
for (i in names(spatial_data)) {

  if (i %in% c("parcels", "roads100", "roads300")) {

  } else {

    print(i)
    output <- st_join(output, spatial_data[[i]])
    print(nrow(output))

  }

}

# remove duplicates created by overlaps in shapefiles
output <- output %>% filter(!duplicated(id))
output$id <- NULL

# post-join cleaning ----

# specify certain joined indicators
output$ohare_noise[is.na(output$ohare_noise)] <- 0
output$floodplain <- ifelse(!is.na(output$floodplain), 1, 0)
output$withinmr101300 <- ifelse(output$withinmr300 == 1 & output$withinmr100 == 0, 1, 0)

# purge columns
output <- output %>% select(-c("withinmr300", "PINB", "SHAPE_Area"))

# extract x y coordinates
output <- st_transform(output, crs = 4326)
output$centroid_x <- st_coordinates(output$geometry)[, 1]
output$centroid_y <- st_coordinates(output$geometry)[, 2]
output$geometry <- NULL

# add ACS information
output$GEOID <- as.numeric(output$GEOID)
output <- left_join(output, read.csv("O:/CCAODATA/data/raw_data/latest_ACS.csv")[, c(-1, -3)], by = "GEOID")

# Upload table to SQL server ----
CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

if (ask.user.yn.question("Are you certain you want to overwrite DTBL_PINLOCATIONS?") == TRUE) {

  dbWriteTable(CCAODATA, "DTBL_PINLOCATIONS", output, overwrite = TRUE)

}

# disconnect after pulls
dbDisconnect(CCAODATA)