# Create models of point-source inverse-square airport noise falloff

# Matt Jackson, November 2023

library(arrow)
library(ccao)
library(DBI)
library(geoarrow)
library(glue)
library(noctua)
library(sf)
library(sp)
library(dplyr)
library(units)

## READ DATA ===================================================================

# Optionally enable query caching
noctua_options(cache_size = 10)

# Establish connection
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())
print("noctua connection established")

midway_noise <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  "SELECT location AS site, year, avg_noise_level AS noise,
  ST_GeomFromBinary(geometry_3435) as geometry
  FROM spatial.midway_noise_monitor"
)

ohare_noise <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  "SELECT site, year, noise, modeled_omp_build_out_values,
  ST_GeomFromBinary(geometry_3435) as geometry
  FROM spatial.ohare_noise_monitor;"
)

## CLEAN DATA ==================================================================

ohare_noise$geometry <- st_as_sfc(ohare_noise$geometry, crs = 3435)
ohare_noise <- st_as_sf(ohare_noise)
ohare_noise <- mutate(ohare_noise, airport = "ohare")

midway_noise <- midway_noise %>%
  mutate(
    modeled_omp_build_out_values = NA,
    airport = "midway"
  ) %>%
  # match data type of ohare_noise
  mutate(
    noise = as.numeric(noise),
    year = as.integer(year)
  )

midway_noise$geometry <- st_as_sfc(midway_noise$geometry, crs = 3435)
midway_noise <- st_as_sf(midway_noise)

airport <- rbind(ohare_noise, midway_noise)

# treat centroid of each airport as point source of noise
ohare_point <- c(
  site = "ohare",
  latitude = 41.97857577880779,
  longitude = -87.90817373313197
)
midway_point <- c(
  site = "midway",
  latitude = 41.78512649107475,
  longitude = -87.75182050036706
)
aps <- as.data.frame(rbind(ohare_point, midway_point)) %>%
  st_as_sf(coords = c("longitude", "latitude")) %>%
  st_set_crs(4326) %>%
  st_transform(3435)

## SET UP MULTIPLE MODELS ======================================================

# ==============================================================================
# MODEL 0: averaged noise levels from 2011 to 2019
airport_clean_averaged <- airport %>%
  mutate(year = as.numeric(year)) %>%
  filter(year >= 2011 & year <= 2019) %>%
  group_by(site, airport) %>%
  summarize(noise = mean(noise, na.rm = TRUE))

# get distance in feet between each sensor and both airports
sensor_dists <- st_distance(airport_clean_averaged, aps)

airport_clean_averaged$distance_ohare <- sensor_dists[, 1]
airport_clean_averaged$distance_midway <- sensor_dists[, 2]

# Modeling choice: assume a baseline DNL in dB of 50,
# per FAA info about average for "quiet, suburban, residential" environment
# https://www.faa.gov/regulations_policies/policy_guidance/noise/community
BASELINE_DNL <- 50

# Modeling choice: assume ALL noise above baseline DNL for a sensor is from the
# airport the sensor is closest to.

# Intensity of sound falls off with the inverse square of distance from point
# source I = Ax^-2, where A represents W/4pi:
# https://tinyurl.com/c9vkrjxa

# To model this falloff, we convert decibels to absolute intensity (since
# dB is logarithmic), subtract our baseline, and then do a linear regression w/
# intensity as y and inverse square of distance as x.
# Our coefficient minimizes sum of squared error for the 2011-19 sensor averages

# transform distance variable into inverse distance-squared
airport_clean_averaged$isq_dist_ohare <-
  (airport_clean_averaged$distance_ohare)^-2
airport_clean_averaged$isq_dist_midway <-
  (airport_clean_averaged$distance_midway)^-2

## Interconversion functions for dB and intensity

I_ZERO <- 1e-12

# https://tinyurl.com/bde8b7jw
decibel_to_intensity <- function(decibel) {
  10^(decibel / 10) * I_ZERO
}

intensity_to_decibel <- function(I) {
  10 * log10(I / I_ZERO)
}

airport_clean_averaged$total_intensity <-
  decibel_to_intensity(airport_clean_averaged$noise)
airport_clean_averaged$added_intensity <-
  decibel_to_intensity(airport_clean_averaged$noise - BASELINE_DNL)

# Verify against https://www.omnicalculator.com/physics/db

## PREDICT AIRPORT NOISE LEVELS ================================================

# Set intercept at 0, i.e. when linear distance from airport is infinite,
# the absolute intensity of sound emanating from it will be 0
# (Decibel measure will NOT drop to 0; it can keep going to -Inf since 0 dB is
# just a cutoff of human *perception* of sound)

ohare_model0_intensity <- lm(added_intensity ~ 0 + isq_dist_ohare,
  data = airport_clean_averaged %>%
    filter(airport == "ohare")
)
# Adjusted R-squared:  0.4002

midway_model0_intensity <- lm(added_intensity ~ 0 + isq_dist_midway,
  data = airport_clean_averaged %>%
    filter(airport == "midway")
)
# Adjusted R-squared:  0.8519

# for future reference:
model0_coefficients <- cbind(
  airport = c("ohare", "midway"),
  coefficient = c(
    ohare_model0_intensity$coefficients,
    midway_model0_intensity$coefficients
  )
)
row.names(model0_coefficients) <- NULL

# To get predicted DNL level for a PIN:
# for each airport:
# -take the PIN's distance to that airport to the -2nd power (=1/(d^2))
# -apply coefficient to that inverse-square distance
# -convert result to decibels to get estimated decibels added by that airport
# -if decibel level is below 0, use 0 instead
# -add O'Hare and Midway results together
# -add BASELINE_DNL (50) to that sum

# ==============================================================================
# MODEL 1: most recent values
# (O'Hare Modernization Project levels for O'Hare; 2019 levels for Midway)

ohare_recent <- ohare_noise %>%
  select(-c(noise, year)) %>%
  distinct() %>%
  rename("noise" = "modeled_omp_build_out_values") %>%
  filter(!(is.na(noise))) # four sensors have NA values for OMP; removed those

midway_recent <- midway_noise %>%
  select(-modeled_omp_build_out_values) %>%
  filter(year == 2019) %>%
  select(-year)

airport_recent <- rbind(ohare_recent, midway_recent)

sensor_dists1 <- st_distance(airport_recent, aps)

airport_recent$distance_ohare <- sensor_dists1[, 1]
airport_recent$distance_midway <- sensor_dists1[, 2]

airport_recent$isq_dist_ohare <- (airport_recent$distance_ohare)^-2
airport_recent$isq_dist_midway <- (airport_recent$distance_midway)^-2

airport_recent$total_intensity <- decibel_to_intensity(airport_recent$noise)
airport_recent$added_intensity <- decibel_to_intensity(
  airport_recent$noise - BASELINE_DNL
)

ohare_model1_intensity <- lm(added_intensity ~ 0 + isq_dist_ohare,
  data = airport_recent %>%
    filter(airport == "ohare")
)
summary(ohare_model1_intensity)
# Adjusted R-squared: 0.6612

midway_model1_intensity <- lm(added_intensity ~ 0 + isq_dist_midway,
  data = airport_recent %>%
    filter(airport == "midway")
)
summary(midway_model1_intensity)
# Adjusted R-squared: 0.8132

model1_coefficients <- cbind(
  airport = c("ohare", "midway"),
  coefficient = c(
    ohare_model1_intensity$coefficients,
    midway_model1_intensity$coefficients
  )
)
row.names(model1_coefficients) <- NULL

# ==============================================================================
# MODEL 1.5: same as above, but using recorded 2019 values for both airports

ohare_19 <- ohare_noise %>%
  select(-modeled_omp_build_out_values) %>%
  filter(year == 2019) %>%
  select(-year)

airport_19 <- rbind(ohare_19, midway_recent)

sensor_dists1 <- st_distance(airport_19, aps)

airport_19$distance_ohare <- sensor_dists1[, 1]
airport_19$distance_midway <- sensor_dists1[, 2]

airport_19$isq_dist_ohare <- (airport_19$distance_ohare)^-2
airport_19$isq_dist_midway <- (airport_19$distance_midway)^-2

airport_19$total_intensity <- decibel_to_intensity(airport_19$noise)
airport_19$added_intensity <- decibel_to_intensity(
  airport_19$noise - BASELINE_DNL
)

ohare_model1_5_intensity <- lm(added_intensity ~ 0 + isq_dist_ohare,
  data = airport_19 %>%
    filter(airport == "ohare")
)
summary(ohare_model1_5_intensity)
# Adjusted R-squared: 0.5066

midway_model1_5_intensity <- lm(added_intensity ~ 0 + isq_dist_midway,
  data = airport_19 %>%
    filter(airport == "midway")
)
summary(midway_model1_5_intensity)
# Adjusted R-squared: 0.8132


model1_5_coefficients <- cbind(
  airport = c("ohare", "midway"),
  coefficient = c(
    ohare_model1_5_intensity$coefficients,
    midway_model1_5_intensity$coefficients
  )
)
row.names(model1_5_coefficients) <- NULL

# ==============================================================================
# MODEL 2: model with year as a separate (continuous) independent variable

airport_all <- airport %>%
  mutate(year = as.numeric(year)) %>%
  filter(year >= 2011 & year <= 2019)

airport_all$distance_ohare <- st_distance(
  airport_all$geometry,
  aps$geometry[1]
)
airport_all$distance_midway <- st_distance(
  airport_all$geometry,
  aps$geometry[2]
)

airport_all$isq_dist_ohare <- (airport_all$distance_ohare)^-2
airport_all$isq_dist_midway <- (airport_all$distance_midway)^-2

airport_all$total_intensity <- decibel_to_intensity(airport_all$noise)
airport_all$added_intensity <- decibel_to_intensity(
  airport_all$noise - BASELINE_DNL
)

ohare_model2_intensity <- lm(added_intensity ~ 0 + isq_dist_ohare + year,
  data = airport_all %>% filter(airport == "ohare")
)
summary(ohare_model2_intensity)
# Adjusted R-squared: 0.3734

midway_model2_intensity <- lm(added_intensity ~ 0 + isq_dist_midway + year,
  data = airport_all %>%
    filter(airport == "midway")
)
summary(midway_model2_intensity)
# Adjusted R-squared: 0.7553


model2_coefficients <- cbind(
  airport = c("ohare", "midway"),
  coefficient = c(
    ohare_model2_intensity$coefficients,
    midway_model2_intensity$coefficients
  )
)
row.names(model2_coefficients) <- NULL

# ==============================================================================
# MODEL 3: model with each year as a fixed factor & OMP numbers as pseudo-"year"

all_omp <- ohare_noise %>%
  select(-c(year, noise)) %>%
  mutate(year = "omp") %>%
  distinct() %>%
  rename("noise" = "modeled_omp_build_out_values")

airport_all_factor <- airport_all %>%
  select(c(site, year, noise, airport, geometry))

airport_all_factor <- rbind(airport_all_factor, all_omp)

airport_all_factor$distance_ohare <- st_distance(
  airport_all_factor$geometry,
  aps$geometry[1]
)
airport_all_factor$distance_midway <- st_distance(
  airport_all_factor$geometry,
  aps$geometry[2]
)

airport_all_factor$isq_dist_ohare <- (airport_all_factor$distance_ohare)^-2
airport_all_factor$isq_dist_midway <- (airport_all_factor$distance_midway)^-2

airport_all_factor$total_intensity <- decibel_to_intensity(
  airport_all_factor$noise
)
airport_all_factor$added_intensity <- decibel_to_intensity(
  airport_all_factor$noise - BASELINE_DNL
)

ohare_model3_intensity <- lm(
  added_intensity ~ 0 + isq_dist_ohare + factor(year),
  data = airport_all_factor %>%
    filter(airport == "ohare")
)
summary(ohare_model3_intensity)
# Adjusted R-squared: 0.3585

midway_model3_intensity <- lm(
  added_intensity ~ 0 + isq_dist_midway + factor(year),
  data = airport_all_factor %>%
    filter(airport == "midway")
)
summary(midway_model3_intensity)
# Adjusted R-squared: 0.7702


model3_coefficients <- cbind(
  airport = c("ohare", "midway"),
  coefficient = c(
    ohare_model2_intensity$coefficients,
    midway_model2_intensity$coefficients
  )
)
row.names(model3_coefficients) <- NULL
