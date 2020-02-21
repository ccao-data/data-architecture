# This script extracts XY coordinates for PINs from a Cook COunty parcels parcelsfile.

user <- Sys.info()[['user']]
source(paste0("C:/Users/",user,"/Documents/ccao_utility/code.r/99_utility_2.r"))
invisible(check.packages(libs))
dirs <- directories("ccao_sf_cama_dev")
options(java.parameters = "-Xmx24g")
database <- 1

# Load geographic data
parcels <- read_sf(dsn = paste0(dirs$spatial_data, "Historical_Parcels__2019.shp"), lay = "Historical_Parcels__2019")
tracts <- read_sf(dsn = paste0(dirs$spatial_data, "tl_2018_17_tract.shp"), lay = "tl_2018_17_tract")
pumas <- read_sf(dsn = paste0(dirs$spatial_data, "ipums_puma_2010.shp"), lay = "ipums_puma_2010")[c(-3, -7)]

leyden_ohare <- read_sf(dsn = paste0(dirs$spatial_data, "Half mile buffer.shp"), lay = "Half mile buffer")
floodplains <- st_read(dsn = paste0(dirs$spatial_data, "NFHL_17_20190515/NFHL_17_20190515.gdb"), layer = "S_Fld_Haz_Ar")
roads <- read_sf(dsn = paste0(dirs$spatial_data, "Streets.shp"), lay = "Streets")

wards <- read_sf(dsn = paste0(dirs$spatial_data, "wards.shp"), lay = "wards")
commissioner <- read_sf(dsn = paste0(dirs$spatial_data, "commissioner.shp"), lay = "commissioner")
repres <- read_sf(dsn = paste0(dirs$spatial_data, "repres_dict.shp"), lay = "repres_dict")
senate  <- read_sf(dsn = paste0(dirs$spatial_data, "senate_dict.shp"), lay = "senate_dict")
tif <- read_sf(dsn = paste0(dirs$spatial_data, "tif_dict.shp"), lay = "tif_dict")
municipalities <- read_sf(dsn = paste0(dirs$spatial_data, "tl_2013_17_place.shp"), lay = "tl_2013_17_place")

# Record parcels where PIN10 does not match Name and create observations for both "Name" and "PIN10" values
problems <- subset(parcels, (substr(Name, 1, 10) != PIN10 | nchar(Name) != 14))

temp <- problems
temp$geometry <- temp$OBJECTID <- NULL
write.csv(temp, file = "O:/CCAODATA/data/bad_data/parcel_problems.csv", row.names = FALSE, na = "")
rm(temp)

problems <- subset(problems, nchar(problems$Name) == 14 & nchar(problems$PIN10) == 10)
names <- problems$Name
problems$Name <- paste0(problems$PIN10, "0000")
problems$PIN10 <- substr(names, 1, 10)
parcels <- rbind(parcels, problems)
rm(names, problems)

# Trim off data we can't use ----
parcels <- subset(parcels, nchar(Name) == 14)

# Set shapefiles to same CRS
parcels <- st_transform(parcels, crs = 3435)
tracts <- st_transform(tracts, crs = 3435)
pumas <- st_transform(pumas, crs = 3435)

leyden_ohare <- st_transform(leyden_ohare, crs = 3435)
floodplains <- st_transform(floodplains, crs = 3435)
roads <- st_transform(roads, crs = 3435)

wards <- st_transform(wards, crs = 3435)
commissioner <- st_transform(commissioner, crs = 3435)
repres <- st_transform(repres, crs = 3435)
senate <- st_transform(senate, crs = 3435)
tif <- st_transform(tif, crs = 3435)
municipalities <- st_transform(municipalities, crs = 3435)

# Identify which pins are affected by O'Hare noise
join <- st_intersection(st_make_valid(parcels), leyden_ohare)
parcels$ohare_noise <- 0
parcels$ohare_noise[parcels$Name %in% join$Name] <- 1
rm(join)

# Identify which pins are in floodplains
join <- st_intersection(st_make_valid(parcels), subset(floodplains, floodplains$SFHA_TF == "T"))
parcels$floodplain <- 0
parcels$floodplain[parcels$Name %in% join$Name] <- 1
rm(join)

# Identify which pins are in a 100ft or 101 to 300ft buffer around major roads
join <- st_intersection(st_make_valid(parcels), st_buffer(subset(roads, MAJORROAD == "YES"), 100))
parcels$withinmr100 <- 0
parcels$withinmr100[parcels$Name %in% join$Name] <- 1
rm(join)

join <- st_intersection(st_make_valid(parcels), st_buffer(subset(roads, MAJORROAD == "YES"), 300))
parcels$withinmr101300 <- 0
parcels$withinmr101300 <- if_else(parcels$withinmr100 != 1 & parcels$Name %in% join$Name, 1, parcels$withinmr101300)
rm(join)

# Drop observations with missing PINs ----
parcels <- parcels[!is.na(parcels$PIN10), ]

# Identify PINs with "999" values
parcels$PIN999 <- ifelse(parcels$PINB == 999, 1, 0)

# Temporarily drop pins that are less than 0.5 sqft in area and identify them
temp <- parcels[parcels$ShapeSTAre < 0.5, ]
temp$centroid_x <- temp$centroid_y <- temp$TRACTCE <- temp$ward <- temp$GEOID.x <- temp$district <- temp$district_1 <- temp$senatedist <- temp$agencynum <- temp$PUMA <- temp$NAME.y <- temp$PLACEFP <- NA
temp$geometry <- NULL
temp$point_parcel <- 1

parcels <- parcels[parcels$ShapeSTAre >= 0.5, ]
parcels$point_parcel <- 0

# Extract x y coordinates if parcels are greater than 0.5 sqft in area
parcels$geometry <- st_centroid(parcels$geometry)
parcels$clean <- gsub("[c()]", "", parcels$geometry)
parcels$centroid_x <- str_split_fixed(parcels$clean, ", ", 2)[, 1]
parcels$centroid_y <- str_split_fixed(parcels$clean, ", ", 2)[, 2]
parcels$clean <- NULL

# Join parcel centroids with census boundaries
parcels <- st_join(parcels, tracts)
parcels <- st_join(parcels, pumas)

# Join parcel centroids with political districts
parcels <- st_join(parcels, wards)
parcels <- st_join(parcels, commissioner)
parcels <- st_join(parcels, repres)
parcels <- st_join(parcels, senate)
parcels <- st_join(parcels, tif)
parcels <- st_join(parcels, municipalities)

# Convert coordinates to lat/long for export and leaflet
parcels <- st_transform(parcels, crs = 4326)
parcels$clean <- gsub("[c()]", "", parcels$geometry)
parcels$centroid_x <- str_split_fixed(parcels$clean, ", ", 2)[, 1]
parcels$centroid_y <- str_split_fixed(parcels$clean, ", ", 2)[, 2]
parcels$clean <- NULL

# Clean up before export & add back pins that are less than 0.5 sqft in area
keep <- c("Name", "PIN999", "point_parcel", "centroid_x", "centroid_y", "TRACTCE", "ward", "ohare_noise", "floodplain", "withinmr100", "withinmr101300", "GEOID.x", "district", "district_1", "senatedist", "agencynum", "PUMA", "NAME.y", "PLACEFP")
parcels <- dplyr::select(parcels, keep)
parcels$geometry <- NULL

temp <- dplyr::select(temp, keep)
parcels <- rbind(parcels, temp)
parcels <- plyr::rename(parcels, c("Name" = "PIN", "GEOID.x" = "GEOID", "district" = "commissioner_dist", "district_1" = "reps_dist", "senatedist" = "senate_dist", "agencynum" = "tif_agencynum", "NAME.y" = "municipality", "PLACEFP" = "FIPS"))

# Adding ACS information in the parcels data
full_data <- read.csv("O:/CCAODATA/data/raw_data/latest_ACS.csv")[, c(-1, -3)]
parcels <- merge(parcels, full_data, by.x = "GEOID", by.y = "GEOID", all.x = TRUE)

# Identify duplicate pins
parcels$PIN10 <- substr(parcels$PIN, 1, 10)
parcels$multiple_geographies <- ifelse ((duplicated(parcels$PIN10) | duplicated(parcels$PIN10, fromLast = TRUE)) == TRUE, 1, 0)
parcels$primary_polygon <- ifelse (duplicated(parcels$PIN10) == FALSE, 1, 0)
parcels$PIN10 <- NULL

# Upload table to SQL server
CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

if (ask.user.yn.question("Are you certain you want to overwrite DTBL_PINLOCATIONS?") == TRUE) {
  
  dbWriteTable(CCAODATA, "DTBL_PINLOCATIONS", parcels, overwrite = TRUE)
  
}

# disconnect after pulls
dbDisconnect(CCAODATA)

# Remove extra files
rm(temp, tracts, pumas, leyden_ohare, floodplains, roads, wards, commissioner, repres, senate, tif, full_data)
