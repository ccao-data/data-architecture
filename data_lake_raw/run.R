library(stringr)
library(tidycensus)
library(dplyr)
library(purrr)
library(openxlsx)
library(arrow)
library(here)
library(sf)

# retrieve raw census data
source(here("scripts/census-asc.R"))
