library(dplyr)
library(glue)
library(purrr)
library(rvest)
library(stringr)
library(xml2)
source("utils.R")

# This script retrieves and uploads PDFs containing
# Midway noise level measurements and addresses of sensors
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")

web_page <-
  "https://www.flychicago.com/community/MDWnoise/ANMS/Pages/ANMSreports.aspx"

# Retrieve URLs and preferred file names for .pdf files
files <- data.frame(
  "url" = paste0(
    "https://www.flychicago.com",
    xml2::read_html(web_page) %>%
      html_nodes("a") %>%
      html_attr("href") %>%
      str_subset("pdf|PDF")
  ),
  "name" = xml2::read_html(web_page) %>%
    rvest::html_nodes("a") %>%
    rvest::html_text() %>%
    str_subset("-Q")
)

# Function to download files and write to S3
down_up <- function(url, file_name) {
  print(glue("Uploading data for: {file_name}"))

  remote_file <- file.path(
    output_bucket, "midway_noise_monitor", glue("{file_name}.pdf")
    )

  if (!aws.s3::object_exists(remote_file)) {
    tmp_file <- tempfile(fileext = ".pdf")

    print(file_name)

    # Grab file
    download.file(url, destfile = tmp_file)

    # Write to S3
    save_local_to_s3(remote_file, tmp_file)
    file.remove(tmp_file)
  }
}

# Apply function to foreclosure data
walk2(files$url, files$name, ~ down_up(.x, .y))
