library(here)
library(rvest)
library(openxlsx)
library(janitor)
library(dplyr)
library(arrow)

# this script retrieves raw DePaul IHS data for the data lake

# scrape main page for .xlsx, which should be most recent release
most_recent_ihs_data_url <- read_html("https://price-index.housingstudies.org/") %>%
  html_nodes(xpath = ".//a[contains(@href, '.xlsx')]") %>%
  html_attr("href") %>%
  sprintf("https://price-index.housingstudies.org%s", .)

# grab the data and clean it just a bit
data.frame(t(

  openxlsx::read.xlsx(most_recent_ihs_data_url, sheet = 2) %>%
    select(-c("X2", "X3", "X4"))

)) %>%

  # names and columns are kind of a mess after the transpose, shift up first row, shift over column names
  row_to_names(1) %>%
  mutate(puma = rownames(ihs_data)) %>%
  dplyr::relocate(puma, .before = "YEARQ") %>%
  rename(name = "YEARQ") %>%

  # write IHS data
  write_parquet(here(
    paste0("s3-bucket/stable/housing/ihs_index/",
           basename(tools::file_path_sans_ext(most_recent_ihs_data_url)),
           ".parquet")
    ))
