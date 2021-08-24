library(here)
library(rvest)
library(openxlsx)
library(janitor)
library(dplyr)
library(arrow)

# this script retrieves raw DePaul IHS data for the data lake
# it assumes a couple things about the imported .xlsx:
# - three unnamed columns renamed "X1", "X2", and "X3" by R and
# - the value of the first row/column being "YEARQ"

# scrape main page for .xlsx, which should be most recent release
most_recent_ihs_data_url <- rvest::read_html("https://price-index.housingstudies.org/") %>%
  rvest::html_nodes(xpath = ".//a[contains(@href, '.xlsx')]") %>%
  rvest::html_attr("href") %>%
  sprintf("https://price-index.housingstudies.org%s", .)

# grab the data and clean it just a bit
data.frame(t(

  openxlsx::read.xlsx(most_recent_ihs_data_url, sheet = 2) %>%
    dplyr::select(-c("X2", "X3", "X4"))

)) %>%

  # names and columns are kind of a mess after the transpose, shift up first row, shift over column names
  janitor::row_to_names(1) %>%
  dplyr::mutate(puma = rownames(.)) %>%
  dplyr::relocate(puma, .before = "YEARQ") %>%
  dplyr::rename(name = "YEARQ") %>%

  # write IHS data
  arrow::write_parquet(here(
    paste0("s3-bucket/stable/housing/ihs_index/",
           basename(tools::file_path_sans_ext(most_recent_ihs_data_url)),
           ".parquet")
    ))

# clean
rm(list = ls())