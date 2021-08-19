library(rvest)
library(magrittr)
library(openxlsx)
library(janitor)
library(dplyr)

# this script retrieves raw DePaul IHS data for the data lake

# scrape main page for .xlsx, which should be most recent release
most_recent_ihs_data_url <- read_html("https://price-index.housingstudies.org/") %>%
  html_nodes(xpath = ".//a[contains(@href, '.xlsx')]") %>%
  html_attr("href") %>%
  sprintf("https://price-index.housingstudies.org%s", .)

# grab the data and clean it just a bit
ihs_data <- openxlsx::read.xlsx(most_recent_ihs_data_url, sheet = 2) %>%
  row_to_names(1)
