library(stringr)
library(tidycensus)
library(dplyr)
library(purrr)
library(openxlsx)
library(arrow)
library(here)
library(sf)
library(here)
library(rvest)
library(janitor)
library(odbc)
library(DBI)

# source scripts
lapply(
  list.files(here("scripts"), full.names = TRUE),
  source
  )