# Create spatial interpolation surfaces
# Gabe Morrison
# Note: This script is a more succinct version of an Rmd attached to the 77 issue


library(arrow)
library(aws.s3)
library(ccao)
library(DBI)
library(geoarrow)
library(ggpubr)
library(glue)
library(gstat)
library(noctua)
library(stars)
library(sf)
library(sp)
library(tidycensus)
library(tidyverse)
library(tmap)
library(viridis)


## Part 1: READ, VISUALIZE, AND CLEAN DATA ===================

# READ DATA:

# Read clean and process midway data:
source("scripts-ccao-data-raw-us-east-1/spatial-midway_sound_data.R")

# Access aws:
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

ohare_noise <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  "SELECT site, year, noise, modeled_omp_build_out_values,
  ST_GeomFromBinary(geometry_3435) as geometry

  FROM spatial.ohare_noise_monitor;"
)

ohare_noise$geometry <- st_as_sfc(ohare_noise$geometry, crs = 3435)
ohare_noise <- st_as_sf(ohare_noise)




ohare_contour <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  "SELECT airport, ST_GeomFromBinary(geometry) as geometry
  FROM spatial.ohare_noise_contour;"
)

ohare_contour$geometry <- st_as_sfc(ohare_contour$geometry)
ohare_contour <- st_as_sf(ohare_contour)
ohare_contour <- st_set_crs(ohare_contour, 4326)
ohare_contour <- st_transform(ohare_contour, 3435)



# Check we read correctly:
print(attr(ohare_noise, "sf_column"))
print(ohare_noise, n = 3)
print(st_geometry(ohare_noise))

ohare_noise <- mutate(ohare_noise, airport = "ohare")


town_shp <- st_transform(ccao::town_shp, 3435)

midway <- mdw
mdw_long <- midway %>%
  pivot_longer(-c(locations, address, geometry), names_to = "year", values_to = "noise") %>%
  rename("site" = "locations") %>%
  select(-address) %>%
  mutate(
    year = str_c("20", str_sub(year, 4, 5)),
    modeled_omp_build_out_values = NA,
    airport = "midway"
  ) %>%
  select(site, year, noise, modeled_omp_build_out_values, airport, geometry) %>%
  mutate(noise = as.numeric(noise))


# nolint start
# Visualize airport trends over time:
# Key finding: big drop in 2020 (COVID STOPPING FLIGHTS)
# av_sound_by_year <- ohare_noise %>%
#  group_by(year) %>%
#  summarize(Median = median(noise, na.rm = T), Mean = mean(noise, na.rm =T )) %>%
#  as.data.frame() %>%
#  select(-geometry) %>%
#  pivot_longer(-year, names_to = "Summary Statistic", values_to = "Values")


# ggplot(av_sound_by_year) +
#  geom_line(aes(x = year, y = Values, col = `Summary Statistic`)) +
#  xlab("Year") +
#  ylab("Sound (DNL)") +
#  ggtitle("The areas surrounding O'Hare have become quieter over time")
# nolint end

# Create airport bounding boxes:
airport <- rbind(ohare_noise, mdw_long)

create_ap_bbox <- function(airport_points, mult) {
  bbox <- st_bbox(airport_points, crs = 3435)
  bbox_sf <- st_as_sfc(bbox)

  bbox_final <- st_buffer(bbox_sf, 5280 * mult) # 2 mile buffer

  return(bbox_final)
}

ohare_bbox <- create_ap_bbox(ohare_noise, 2)
midway_bbox <- create_ap_bbox(midway, 3.5)
bbox <- st_as_sf(st_union(ohare_bbox, midway_bbox))


# Get dataset to use for modelling
airport_clean <- airport %>%
  mutate(year = as.numeric(year)) %>%
  filter(year >= 2011 & year <= 2019) %>%
  group_by(site) %>%
  summarize(noise = mean(noise, na.rm = TRUE))



# Create new dataset with Airports included:
ohare <- c(site = "ohare", noise = "90", longitude = 41.97857577880779, latitude = -87.90817373313197)
midway <- c(site = "midway", noise = "90", longitude = 41.78512649107475, latitude = -87.75182050036706)
aps <- as.data.frame(rbind(ohare, midway))
aps <- st_as_sf(aps, coords = c("latitude", "longitude"))
aps <- st_set_crs(aps, 4326)
aps <- st_transform(aps, 3435)

airport_boost <- rbind(airport_clean, aps)



# Create new dataset with boundaries as quiet (50!)
convert_bbox_to_points <- function(bbox) {
  to_sf <- st_as_sf(bbox)
  few_points <- st_simplify(to_sf, dTolerance = 5000)
  points <- st_cast(few_points, to = "POINT")
}

ohare_points_bbox <- convert_bbox_to_points(ohare_bbox)
midway_points_bbox <- convert_bbox_to_points(midway_bbox)

sound <- rep(c("50"), 6)
oh_name <- rep(c("ohare_border"), 6)
sound_md <- rep(c("50"), 9)
md_name <- rep(c("midway_border"), 9)


ohare_bound_sound <- cbind(site = oh_name, noise = sound, ohare_points_bbox)
midway_bound_sound <- cbind(site = md_name, noise = sound_md, midway_points_bbox)

air_bound_sound <- rbind(ohare_bound_sound, midway_bound_sound) %>%
  rename(geometry = x) %>%
  st_as_sf() %>%
  st_set_crs(3435)

airport_extra_boost <- rbind(airport_boost, air_bound_sound)

airport_extra_boost <- airport_extra_boost %>%
  slice(-c(57, 66))



# Add coordinates explicitly:
airport_clean$X <- st_coordinates(st_as_sf(airport_clean))[, 1]
airport_clean$Y <- st_coordinates(airport_clean)[, 2]

airport_boost$X <- st_coordinates(st_as_sf(airport_boost))[, 1]
airport_boost$Y <- st_coordinates(airport_boost)[, 2]


airport_extra_boost$X <- st_coordinates(st_as_sf(airport_extra_boost))[, 1]
airport_extra_boost$Y <- st_coordinates(airport_extra_boost)[, 2]


# Rasterize bounding boxes:
raster_ohare <- stars::st_as_stars(ohare_bbox, dx = 1000)
raster_mdw <- stars::st_as_stars(midway_bbox, dx = 1000)
rasters <- stars::st_mosaic(raster_ohare, raster_mdw)



## PART 2: BUILD SURFACES AND TEST HOW GOOD THEY ARE ====================



plot_surface <- function(data) {
  # Function to plot surface created
  # input: data - should be the output of either gstats:idw or gstats:krige

  # output: no output but prints a map

  plot <- ggplot() +
    geom_sf(data = town_shp) +
    geom_sf(data = ohare_bbox) +
    geom_sf(data = midway_bbox) +
    geom_stars(data = data, aes(fill = var1.pred, x = x, y = y)) +
    geom_sf(data = ohare_contour, fill = NA, alpha = .5, color = "red") +
    coord_sf(lims_method = "geometry_bbox") +
    scale_fill_viridis(limits = c(40, 85))

  print(plot)
}


# Create crossvalidation code for IDW:
# Code strongly influenced by: https://mgimond.github.io/Spatial/interpolation-in-r.html


compute_idw_rmse <- function(data, target_var, power, sub) {
  # Function to compute RMSE of IDW model
  # Function takes spatial point data with a target column and makes n
  # IDW surfaces where n=number of rows in data
  # It uses n -1 points to make the surface and then evaluates the model
  # on the nth point and then computes the RMSE of the those estimates

  # Inputs:
  #  data: (sf) - spatial POINT dataframe
  # target_var (string) - refers to a column in sf that has values to interpolate
  # power (int) - the power of the IDW measure to use
  # sub (int) - the number of coluns from the bottom of the dataframe
  #             not to use

  # Output:
  # rmse: the Root Mean Square error comparing true values of the point vs
  #       the prediction the model made when holding that point out


  data_df <- as.data.frame(data)
  # Length true data
  l <- nrow(data_df) - sub


  tv <- data_df[1:l, target_var]
  tv <- as.numeric(tv)

  # initialize empty output vector to be filled with predictions
  out <- vector(length = l)

  # Get predictions from surface made without the given point:
  for (i in 1:l) {
    out[i] <- idw(noise ~ 1, data[-i, ], data[i, ], idp = power)$var1.pred
  }

  # Compute RMSE of predictions vs true values
  print(out)
  print(tv)

  rmse <- sqrt(mean((out - tv)^2))
  mae <- mean(abs(out - tv))

  return(c(rmse, mae))
}

# Check code:
# r <- compute_idw_rmse(airport_clean, "noise", 2, sub = 0) # nolint


idw_tune_hyper <- function(data, target_var, idw_params, sub) {
  # Function to compute rmse's for all idw's inputed
  # Inputs:
  #  data: (sf) - spatial POINT dataframe
  # target_var (string) - refers to a column in sf that has values to interpolate
  # idw_params (vector of int) - a vector containing the power of the IDW measure to test
  # sub (int) - the number of rows taht are not true data

  # Outputs:
  # res_df (dataframe) - Column 1 indicates idw power and column 2 shows rmse

  results_rmse <- vector(length = length(idw_params))
  results_mae <- vector(length = length(idw_params))
  print(length(idw_params))
  for (i in seq_along(idw_params)) {
    print(i)
    out <- compute_idw_rmse(data, target_var, idw_params[i], sub)
    rmse <- out[1]
    mae <- out[2]
    print(results_rmse)
    results_rmse[i] <- rmse
    results_mae[i] <- mae
  }
  res_df <- cbind(idw_params, results_rmse, results_mae)

  return(res_df)
}

# Test IDW with different exponents (no extra data):
idw_results <- idw_tune_hyper(
  airport_clean,
  "noise",
  c(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20
  ), 0
)

# Hyperparameter testing with boosted dataset:
idw_results_boosted <- idw_tune_hyper(
  airport_extra_boost,
  "noise",
  c(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20
  ), 15
)


compute_krige_rmse <- function(data,
                               target_var,
                               equation,
                               cutoff,
                               width,
                               funct_form,
                               no_print = TRUE,
                               subtractor = 0) {
  # Function to compute RMSE and MAE of krige model and prints surface is no_print = FALSE

  # Function takes spatial point data with a target column and makes n
  # IDW surfaces where n=number of rows in data
  # It uses n -1 points to make the surface and then evaluates the model
  # on the nth point and then computes the RMSE of the those estimates

  # Inputs:
  #  data: (sf) - spatial POINT dataframe. Note that all points that are NOT
  #               sound recording stations must be the last rows of the
  #               dataframe

  # target_var (string) - refers to a column in sf that has values to interpolate
  # feature_vector (vector of strings) - vector containing specific hyper-
  #                                     parameters to test
  #               Components inside feature_vector:
  #                    equation (string): equation to detrend data
  #                    cutoff (float): cutoff of distance above which to ignore
  #                    width (float): size of bins to create to get measures
  #                                    for sample viariogram
  #                    funct_form (str): description of functional form of model
  #                                       to fit to to sample variogram
  #                    no_print(Boolean): whether a final map should be printed
  #                                       initialized to TRUE
  #                   subtractor (int): reflects the number of points in the
  #                                      dataset that are NOT sound recorders
  #                                      but that have been added to improve
  #                                      model performance


  # Output:
  # output (vector with two elements):
  #   rmse: the Root Mean Square error comparing true values of the point vs
  #       the prediction the model made when holding that point out
  #   mae: mean average error of true values to prediction when used in LOOCV

  # access target variables within data:
  data_df <- as.data.frame(data)
  tv <- data_df[, target_var]
  tv <- tv[1:(length(tv) - subtractor)]

  # Set up equation:
  eq <- as.formula(equation)

  # Iteratively create Krigging surface:
  out <- vector(length = (nrow(data) - subtractor))


  for (i in 1:(nrow(data) - subtractor)) {
    train_data <- data[-i, ]
    test_data <- data[i, ]

    # Create variograms:
    v <- variogram(eq, train_data,
      cutoff = cutoff,
      width = width
    )
    v.m <- fit.variogram(v, vgm(1, funct_form, 5000, 1))


    out[i] <- krige(eq, train_data, test_data, v.m)$var1.pred
  }

  # Compute RMSE and MAE of predictions vs true values
  print(out)
  tv <- as.numeric(tv)

  print(tv)


  rmse <- sqrt(mean((out - tv)^2))
  mae <- mean(abs(out - tv))

  # Handle if to print
  if (no_print == FALSE) {
    v <- variogram(eq, data, cutoff = cutoff, width = width)
    v.m <- fit.variogram(v, vgm(1, funct_form, 50000, 1))
    # v.m = fit.variogram(v, vgm(funct_form)) # nolint
    k <- krige(eq, data, rasters, v.m)

    surface <- plot_surface(k)
  }

  return(c(rmse, mae))
}

# nolint start
# Proof of concept test runs:
# proof_of_concept <- compute_krige_rmse(airport_clean, "noise", "noise~1",  200000, 3000, "Gau", no_print = FALSE)

# proof_of_concept2 <- compute_krige_rmse(airport_boost, "noise", "noise~1",  200000, 3000, "Gau", no_print = FALSE, subtractor = 2)


# proof_of_concept3 <- compute_krige_rmse(airport_extra_boost,
#                                        "noise",
#                                        "noise~1",
#                                        200000,
#                                        3000,
#                                        "Gau",
#                                        no_print = FALSE,
#                                        subtractor = 15)
# nolint end

krige_tune_hyper <- function(data,
                             target_var,
                             formula_list,
                             cutoff_list,
                             width_list,
                             funct_form_list,
                             sub) {
  # Function to compute rmse's for all idw's inputed
  # Inputs:
  #  data: (sf) - spatial POINT dataframe
  # target_var (string) - refers to a column in sf that has values to interpolate
  # *_list (vectors of various types) - all must be same length:
  #     formula (string): formula for spatial model specifications
  #     cutoff (float): to check different cutoffs
  #     width (float): to check different widths
  #     funct_form_list (string): characterize functional form of fitted variogram
  #     sub (int) - the number of additional points added to the data
  #                 0 if nothing
  #                 2 if just airports
  #                 15 if airports and boundary data


  # Outputs:
  # res_df (dataframe) - Column 1 indicates idw power and column 2 shows rmse
  l <- length(formula_list)
  # print(l)
  results_rmse <- vector(length = l)
  results_mae <- vector(length = l)
  names <- vector(length = l)
  for (i in 1:l) {
    print(i)



    out <- compute_krige_rmse(data,
      target_var,
      formula_list[i],
      cutoff_list[i],
      width_list[i],
      funct_form_list[i],
      no_print = TRUE,
      subtractor = sub
    )
    rmse <- out[1]
    mae <- out[2]

    results_rmse[i] <- rmse
    results_mae[i] <- mae

    name <- str_c(
      formula_list[i], "_",
      cutoff_list[i], "_",
      width_list[i], "_",
      funct_form_list[i]
    )
    print(name)
    names[i] <- name
  }

  res_df <- cbind(names, results_rmse, results_mae)

  return(res_df)
}


# Write equations to model space:
eq1 <- "noise ~ 1"
eq2 <- "noise ~ X + Y"
eq3 <- "noise~X + Y + I(X*X)+I(Y*Y) + I(X*Y)"


# Create lists to run:
eq1_l <- c(eq1)
eq1_l <- rep(eq1_l, 24)

eq2_l <- c(eq2)
eq2_l <- rep(eq2_l, 24)

eq3_l <- c(eq3)
eq3_l <- rep(eq3_l, 24)

eqs <- c(eq1_l, eq2_l, eq3_l)

# Create binwidths
binwidth <- c(250, 500, 750, 1000, 2000, 3000)
bw <- rep(binwidth, 12)

# create Funct forms:
gau <- c("Gau")
g6 <- rep(gau, 6)
lin <- c("Lin")
l6 <- rep(lin, 6)
sph <- c("Sph")
s6 <- rep(sph, 6)
mat <- c("Mat")
m6 <- rep(mat, 6)

f24 <- c(g6, l6, s6, m6)
f72 <- rep(f24, 3)

max_size <- rep(c(200000), 72)


# Small model run
# Test idw_tune_hyper performance:
small_model_run <- krige_tune_hyper(airport_extra_boost,
  "noise",
  c(eq1, eq3),
  c(200000, 200000),
  c(1500, 1500),
  c("Gau", "Gau"),
  sub = 15
)


# Big model run:
# test
big_model_run <- krige_tune_hyper(airport_extra_boost,
  "noise",
  eqs,
  max_size,
  bw,
  f72,
  sub = 15
)


# Note: Ran with different airport sound levels:
# With airport 80 - RMSE best: 4.32714518137944
# With airport 90- RMSE best: 3.86
# WIth airport 100- RMSE best: 4.23075692883907
# with airport 110 - RMSE best:4.65673463236301


# Test model no extra points:
model_run_no_boost <- krige_tune_hyper(airport_clean,
  "noise",
  eqs,
  max_size,
  bw,
  f72,
  sub = 0
)


# PART 4: FROM BEST MODEL MAKE FINAL GRIDS =========================

# Get final outputs:
# Create really fine-grained rasters
raster_ohare_100 <- stars::st_as_stars(ohare_bbox, dx = 100)
raster_mdw_100 <- stars::st_as_stars(midway_bbox, dx = 100)
rasters_100 <- stars::st_mosaic(raster_ohare_100, raster_mdw_100)


# Functions to make krigging surfaces for many years:

create_year_kriging <- function(data, year_num, raster_file) {
  # Function to create a krigging surface with the best performing krige model
  # specifications given a year of the data
  # This function also uses the "boosted data" of the airport with the bounding
  # added data

  # Inputs:
  # Data: likely "airport" it should be a sf dataframe with columns
  # "site" and "noise"
  # year_num - string - the year of the data to use. EX: "2012"
  # raster_file - starts raster object - likely either "rasters" or "rasters_100"
  #           this needs to be an underlying set of cells to predict on

  # Output:
  # Returns the surface with predictions from the krige surface
  # IF UNCOMMENTED:
  #     Puts a pdf of the surface in "output/krig_demo/<year_num>.pdf"
  #     Prints the surface

  clean_data <- data %>%
    filter(year == year_num) %>%
    select(site, noise) %>%
    filter(!is.na(noise))

  clean_data <- rbind(clean_data, air_bound_sound)
  clean_data <- rbind(clean_data, aps)
  clean_data <- unique(clean_data)


  v <- variogram(noise ~ 1, clean_data, cutoff = 200000, width = 500)
  v.m <- fit.variogram(v, vgm(1, "Gau", 5000, 1))
  k <- krige(noise ~ 1, clean_data, raster_file, v.m)
  # nolint start
  # pdf(str_c("output/krig_demo/", year_num, ".pdf"), width = 11, height = 8.5)

  # plot_surface(k)

  # dev.off()
  # # nolint end
  return(k)
}


create_av_kriging <- function(data, raster_file) {
  # Function to create a krigging surface with the best performing krige model
  # specifications for all years of data
  # This function also uses the "boosted data" of the airport with the bounding
  # added data

  # Inputs:
  # Data: likely "airport" it should be a sf dataframe with columns
  # "site" and "noise"
  # raster_file - starts raster object - likely either "rasters" or "rasters_100"
  #           this needs to be an underlying set of cells to predict on

  # Output:
  # Returns the surface with predictions from the krige surface
  clean_data <- data %>%
    filter(year %in% c(
      "2011", "2012", "2013", "2014", "2015", "2016",
      "2017", "2018", "2019"
    )) %>%
    select(site, noise) %>%
    filter(!is.na(noise)) %>%
    group_by(site) %>%
    summarize(noise = mean(noise))

  clean_data <- rbind(clean_data, air_bound_sound)
  clean_data <- rbind(clean_data, aps)
  clean_data <- unique(clean_data)

  v <- variogram(noise ~ 1, clean_data, cutoff = 200000, width = 500)
  v.m <- fit.variogram(v, vgm(1, "Gau", 5000, 1))
  k <- krige(noise ~ 1, clean_data, raster_file, v.m)

  return(k)
}


# Year-by-year with best performing model:
years <- c(
  "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017",
  "2018", "2019", "2020"
)

out <- lapply(years, create_year_kriging, raster_file = rasters_100, data = airport)

# Demo from 2010:
# plot_surface(out[[1]]) # nolint


# Create surface for 2019 Midway and OMP modelled out Ohare as a final surface:
# modify data to create a single column combining OMP modelled values for
# OHare and 2019 sound values for Midway
airport_final <- airport %>%
  filter(year == 2019) %>%
  mutate(noise_final = noise) %>%
  mutate(noise_final = ifelse(airport == "ohare",
    modeled_omp_build_out_values,
    noise_final
  )) %>%
  select(site, noise = noise_final, geometry) %>%
  filter(!is.na(noise))

# Add in extra values - RUNNING TO RIGHT HERE!
airport_final <- rbind(airport_final, air_bound_sound)
airport_final <- rbind(airport_final, aps)
airport_final <- unique(airport_final)

airport_final <- mutate(airport_final, noise = as.numeric(noise))




# Create surface for it:
v <- variogram(noise ~ 1, airport_final, cutoff = 200000, width = 500)

v.m <- fit.variogram(v, vgm(1, "Gau", 5000, 1))
k_combo <- krige(noise ~ 1, airport_final, rasters_100, v.m)


## PART 5: PULL PIN DATA, DO SPATIAL JOIN, SAVE AS CSV ===================


spatial_join_raster_pin <- function(year) {
  # Function to
  # (1) query DB and get pins from a given year
  # (2) join raster value to the pin
  # (3) write to csv

  # Inputs:
  # year (string) - the year of data to query

  # Output:
  # No output but writes csv file with pin and DNL value
  # Also has some friendly prints (this takes a while to run)

  print("YEAR IS:")
  print(year)



  # raster_location <- str_c("output/kriging_surfaces/rasters/", year, ".tif") # nolint
  # raster_file <- read_stars(raster_location) # nolint
  ind <- as.numeric(year) - 2009

  raster_file <- out[[ind]]
  # this ^ converts "2010" into index 1 of ouc

  print("read raster")

  parcel_query_string <- str_c("Select pin10, x_3435, y_3435 FROM spatial.parcel
    Where year = ", "'", year, "'")



  parcels <- dbGetQuery(
    conn = AWS_ATHENA_CONN_NOCTUA,
    parcel_query_string
  )
  print("passed sql")

  parcels_geo <- st_as_sf(parcels, coords = c("x_3435", "y_3435"))
  print("converted to sf")

  st_crs(parcels_geo) <- 3435

  print("set crs to 3435")

  parcels_geo_spj <- st_join(parcels_geo, st_as_sf(raster_file))

  print("performed spatial join")


  parcels_spj <- parcels_geo_spj %>%
    as.data.frame() %>%
    dplyr::rename(noise = 2) %>%
    dplyr::select(pin10, noise) %>%
    dplyr::mutate(noise = ifelse(is.na(noise), 55,
      ifelse(noise < 55, 55, noise)
    ))

  print("cleaned")


  file_name <- str_c("surface_", year, ".csv")
  write.csv(parcels_spj, file_name)
  print("wrote")
}

years <- c("2015", "2016", "2017", "2018", "2019", "2020")
years_low <- c("2010", "2011", "2012", "2013", "2014")


lapply(years_low, spatial_join_raster_pin)
lapply(years, spatial_join_raster_pin)



# Same code as above but for new year:
year <- "2021"
raster_location <- str_c("output/kriging_surfaces/rasters/", "omp_19.tif")
raster_file <- read_stars(raster_location)

print("read raster")

parcel_query_string <- str_c("Select pin10, x_3435, y_3435 FROM spatial.parcel
  Where year = ", "'", year, "'")



parcels <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  parcel_query_string
)
print("passed sql")

parcels_geo <- st_as_sf(parcels, coords = c("x_3435", "y_3435"))
print("converted to sf")

st_crs(parcels_geo) <- 3435

print("set crs to 3435")

parcels_geo_spj <- st_join(parcels_geo, st_as_sf(k_combo))

print("performed spatial join")


parcels_spj <- parcels_geo_spj %>%
  as.data.frame() %>%
  dplyr::rename(noise = 2) %>%
  dplyr::select(pin10, noise) %>%
  dplyr::mutate(noise = ifelse(is.na(noise), 55,
    ifelse(noise < 55, 55, noise)
  ))

print("cleaned")


file_name <- str_c("surface_", year, ".csv")
write.csv(parcels_spj, file_name)
print("wrote")



# Calculate aggregate stats of parcels by DNL:
ag_stats_parcels_21 <- parcels_spj %>%
  mutate(sound_bin = cut(noise,
    breaks = c(50, 55, 60, 65, 70, 75, 80, 85, 90, 150)
  )) %>%
  group_by(sound_bin) %>%
  summarize(
    count = n(),
    percent = n() / nrow(parcels_spj)
  ) %>%
  mutate(percent = round(percent, 3) * 100)

DNL <- c(
  "50 - 55",
  "55 - 60",
  "60 - 65",
  "65 - 70",
  "70 - 75",
  "75 - 80",
  "80 - 85"
)
ag_stats_parcels_21 <- cbind(ag_stats_parcels_21, DNL) %>%
  select(DNL, count, percent)

write.csv(ag_stats_parcels_21, "exposure_DNL_21.csv")



# Reading:
# https://keen-swartz-3146c4.netlify.app/interpolation.html#sample-variogram
# http://132.72.155.230:3838/r/spatial-interpolation-of-point-data.html
# https://mgimond.github.io/Spatial/spatial-interpolation.html


# Notes on DNL:
#  From FAA: https://www.faa.gov/regulations_policies/policy_guidance/noise/community/
#  They describe a quiet suburban neighborhood as having a DNL of 50.

# Also from FAA: https://www.faa.gov/regulations_policies/policy_guidance/noise/basics/
#  between 25 and 30 - library/bedroom at night
# between 55 and 60 - inside with a laundry machine going in the next room
# between 70 and 75 - busy highway 50 ft away
# between 105 - 110 - 3ft from a car honking its horn
