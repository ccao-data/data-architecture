#Create Sound data from reports from midway airport:

library(tidyverse)
library(tabulizer)
library(tidygeocoder)
library(sf)
library(tmap)
library(ccao)

#Data Copied from reports here: https://www.flychicago.com/community/MDWnoise/ANMS/Pages/ANMSreports.aspx


address <- c("3701 S. 58th Ct. Cicero IL", 
               "5159 1/2 S. Kenneth Ave. Chicago IL", 
               "4504 1/2 W. 65th St. Chicago IL",
               "5701 1/2 W. 64th St. Chicago IL",
               "5359 S. Newland Ave. Chicago IL",
               "5159 1/2 S. Menard Ave. Chicago IL",
               "3457 1/2 W. 76th Place Chicago IL",
               "8056 1/2 Lockwood Ave. Burbank IL",
               "8052 1/2 S. Oketo Ave. Bridgeview IL",
               "7517 W. 61st St. Summit IL",
               "4308 Wenonah Ave. Stickney IL",
               "5250 1/2 S. Homan Ave. Chicago IL",
               "3942 S. Albany Ave. Chicago IL")

locations <- c(301, 302, 303, 304, 305, 306, 308, 309, 310, 311, 312, 313, 314)


#Alternative:
av_07 <- c(53.4, 70.2, 66.3, 73.7, 58.6, 67.8, 63.7, 54.4, 60.7, 57.1, 50.4, 59.9, NA)
av_08 <- c(51.4, 69.0, 66.3, 71.0, 58.2, 68.1, 63.9, 45.5, 60.2, 58.2, 52.3, 58.2, NA)
av_09 <- c(51.6, 68.6, 69.6, 73.3, 59.8, 66.7, 62.1, 44.3, 61.9, 54.2, 51.5, 58.6, NA)
av_10 <- c(51.0, 68.2, 65.7, 72.6, 58.9, 61.2, 62.9, 45.0, (59.3 + 56.4) /2, 54.7, 52.1, 58.2, NA)
av_11 <- c(51.5, 69.2, 65.3, 68.7, 59.4, 62.9, 62.5, 46.0, 61.3, 54.4, 49.0, 58.9, NA)
av_12 <- c(51.3, 68.5, 65.5, 71.0, 60.5, 63.3, 62.8, 46.7, 59.9, 54.4, 48.3, 57.4, NA)
av_13 <- c(51.3, 68.5, 65.5, 71.0, 60.5, 63.3, 62.8, 46.7, 59.9, 54.4, 48.3, 57.4, NA)
av_14 <- c(53.2, 70.0, 65.2, 67.2, 62.3, 64.8, 62.0, 52.7, 59.9, 54.9, 51.4, 58.2, NA)
av_15 <- c(55.0, 70.6, 66.2, 74.1, 63.5, 70.9, 63.1, 53.5, 61.1, 52.6, 51.7, 62.1, (60.5 + 63.0)/2)
av_16 <- c(54.7, 71.2, 67.4, 73.4, 55.2, 70.2, 63.4, 54.9, 61.0, 55.3, 54.9, 58.8, 61.2)
av_17 <- c(55.4, 71.2, 66.8, 73.0, 55.6, 70.2, 62.9, 55.1, 60.3, 55.9, 54.4, 59.0, 61.3)
av_18 <- c(55.1, 70.8, 65.5, 73.6, 55.2, 70.0, 60.9, 54.4, 61.2, 55.2, 53.2, 59.7, 60.6)
av_19 <- c(55.1, 70.9, 65.5, 71.4, 54.6, 69.2, 60.9, 55.2, 60.3, 55.0, 50.8, 59.0, 61.1)
av_20 <- c(52.4, 66.7, 61.9, 67.3, 51.9, 64.1, 58.5, 50.9, 56.6, 52.1, 48.4, 54.3, 58.2)
av_21 <- c(50.7, 70.1, 63.1, 72.8, 55.8, 65.6, 58.8, 53.9, 58.7, 53.1, 51.1, 58.1, 59.0)


mdw_data <- cbind(locations, address, av_07, av_08, av_09, av_10, 
                          av_11, av_12, av_13, av_14, av_15, av_16, av_17, 
                          av_18, av_19, av_20, av_21)


#Geocode addresses:
mdw_lat_long <- mdw_data %>%
  as.data.frame() %>% 
  geocode(address, method = 'arcgis', lat = latitude , long = longitude) 

#Convert ot 3435
mdw <- mdw_lat_long %>%
  st_as_sf(coords = c("longitude", "latitude")) %>%
  st_set_crs(4326) %>%
  st_transform(crs= st_crs(3435))


#Visualize to check reasonable-ness
town_shp <- st_transform(ccao::town_shp, 3435)

tm_shape(town_shp) + tm_polygons() + 
  tm_shape(mdw) + tm_dots() +
  tm_scale_bar()

#write file:
st_write(mdw, "input/midway_sound.geojson")

