# Create model of point-source inverse-square airport noise falloff

# Matt Jackson, October 2023

library(arrow)
library(aws.s3)
library(ccao)
library(DBI)
library(geoarrow)
library(ggplot2)
library(ggpubr)
library(glue)
library(gstat)
library(here)
library(noctua)
library(stars)
library(sf)
library(sp)
# "The legacy packages maptools, rgdal, and rgeos, underpinning the sp package,
# which was just loaded, will retire in October 2023.
# Please refer to R-spatial evolution reports for details, especially
# https://r-spatial.org/r/2023/05/15/evolution4.html.
# It may be desirable to make the sf package available;
# package maintainers should consider adding sf to Suggests:.
# The sp package is now running under evolution status 2
# (status 2 uses the sf package in place of rgdal)"
library(tidycensus)
library(tidyverse)
library(tmap)
# "Breaking News: tmap 3.x is retiring. Please test v4, e.g. with
# remotes::install_github('r-tmap/tmap')"
library(units)
library(viridis)

## READ DATA ===================================================================

# Optionally enable query caching
noctua_options(cache_size = 10)

# Establish connection
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())
print("noctua connection established")

midway <- read_parquet(
  "s3://ccao-data-warehouse-us-east-1/spatial/environment/midway_noise_monitor/midway_noise_monitor.parquet"
  )

ohare_noise <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  "SELECT site, year, noise, modeled_omp_build_out_values,
  ST_GeomFromBinary(geometry_3435) as geometry
  FROM spatial.ohare_noise_monitor;"
)

ohare_noise$geometry <- st_as_sfc(ohare_noise$geometry, crs = 3435)
ohare_noise <- st_as_sf(ohare_noise)

## CLEAN DATA ==================================================================

ohare_noise <- mutate(ohare_noise, airport = "ohare")

midway_noise <- midway %>%
  rename("site" = "location", "noise" = "avg_noise_level") %>%
  select(-address) %>%
  mutate(modeled_omp_build_out_values = NA,
         airport = "midway") %>%
  select(site, year, noise, modeled_omp_build_out_values, airport, geometry) %>%
  mutate(noise = as.numeric(noise),
         year = as.integer(year)) %>% #match data type of ohare_noise
  st_as_sf(crs=4326) %>% st_transform(3435)

airport <- rbind(ohare_noise, midway_noise)

#Get averaged noise levels for simpler modeling
airport_clean <- airport %>%
  mutate(year = as.numeric(year)) %>%
  filter(year >= 2011 & year <= 2019) %>%
  group_by(site, airport) %>%
  summarize(noise = mean(noise, na.rm = T))

#treat centroid of each airport as point source of noise
ohare_point <- c(site = "ohare", 
                 latitude = 41.97857577880779, 
                 longitude = -87.90817373313197)
midway_point <- c(site = "midway", 
                  latitude= 41.78512649107475, 
                  longitude = -87.75182050036706)
aps <- as.data.frame(rbind(ohare_point, midway_point)) %>%
  st_as_sf(coords = c("longitude", "latitude")) %>% 
  st_set_crs(4326) %>%
  st_transform(3435)

#get distance in feet between each sensor and both airports
sensor_dists <- st_distance(airport_clean, aps)

airport_clean$distance_ohare <- sensor_dists[,1]
airport_clean$distance_midway <- sensor_dists[,2]

# Modeling choice: assume a baseline DNL in dB of 50, 
# per FAA info about average for "quiet, suburban, residential" environment
# https://www.faa.gov/regulations_policies/policy_guidance/noise/community
BASELINE_DNL <- 50

# Modeling choice: assume ALL noise above baseline DNL for a sensor is from the 
# airport the sensor is closest to.

# Intensity of sound falls off with the inverse square of distance from point source
# I = Ax^-2, where A represents W/4pi: 
# https://blog.soton.ac.uk/soundwaves/wave-basics/point-sources-inverse-square-law/

# To model this falloff, we convert decibels to absolute intensity (since
# dB is logarithmic), subtract our baseline, and then do a linear regression with 
# intensity as y and inverse square of distance as x.
# Our coefficient minimizes sum of squared error for the 2011-19 sensor averages

# transform distance variable into inverse distance-squared
airport_clean$isq_dist_ohare <- (airport_clean$distance_ohare)^-2
airport_clean$isq_dist_midway <- (airport_clean$distance_midway)^-2

## Interconversion functions for dB and intensity

I_ZERO = 1e-12

# https://courses.lumenlearning.com/atd-austincc-physics1/chapter/17-3-sound-intensity-and-sound-level/
decibel_to_intensity <- function(decibel){
  return(10^(decibel/10) * I_ZERO)
}

intensity_to_decibel <- function(I){
  return (10 * log10(I/I_ZERO))
}

airport_clean$total_intensity <- decibel_to_intensity(airport_clean$noise)
airport_clean$added_intensity <- decibel_to_intensity(airport_clean$noise - BASELINE_DNL)

# Verify against https://www.omnicalculator.com/physics/db 
# (change "SIL" entry, leave rest same)
# for (i in 1:150){
#   print(paste0("Decibel: ", i, " Intensity: ", decibel_to_intensity(i)))
# }

## PREDICT AIRPORT NOISE LEVELS ================================================

# Set intercept at 0, i.e., when linear distance from airport is infinite,
# the absolute intensity of sound emanating from it will be 0
# (Decibel measure will NOT drop to 0; it can keep going to -Inf since 0 dB is 
# just a a cutoff of human *perception* of sound)

ohare_model_intensity <- lm(added_intensity ~ 0 + isq_dist_ohare, data = airport_clean %>% filter(airport == "ohare")) 
# Adjusted R-squared:  0.4002 

midway_model_intensity <- lm(added_intensity ~ 0 + isq_dist_midway, data = airport_clean %>% filter(airport == "midway")) 
# Adjusted R-squared:  0.8519  

#for use by dbt / Athena:
model_coefficients <- cbind(airport = c("ohare", "midway"),
                            coefficient = c(ohare_model_intensity$coefficients, 
                                            midway_model_intensity$coefficients))
row.names(model_coefficients) = NULL

# To get predicted DNL level for a PIN:
# for each airport:
  # -take the PIN's distance to that airport to the -2nd power (=1/(d^2))
  # -apply coefficient to that inverse-square distance
  # -convert result to decibels to get estimated decibels added by that airport
  # -if decibel level is below 0, use 0 instead
# -add O'Hare and Midway results together
# -add BASELINE_DNL (50) to that sum
