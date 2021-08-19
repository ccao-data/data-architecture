library(stringr)
library(tidycensus)
library(dplyr)
library(purrr)
library(openxlsx)
library(arrow)
library(here)
library(sf)

# retrieve raw census data
source(here("scripts/stable-census-acs.R"))
source(here("scripts/stable-census-decennial.R"))
source(here("scripts/ihs.R"))