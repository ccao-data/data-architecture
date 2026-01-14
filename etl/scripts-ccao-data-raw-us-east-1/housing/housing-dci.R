library(aws.s3)
library(purrr)
source("utils.R")

# This script downloads Distressed Communities Index data from Economic
# Innovation Group. It is a zip-code level measure of economic well-being.

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "housing", "dci")

# URL of the file we want to upload. This should be manually checked to see if
# data more recent than 2024 is included.
urls <- c(
  "2024" = paste0(
    "https://eig.org/dci-maps-2023/data/",
    "1cd12716-de4a-4ef6-884b-af6e1066b581.csv"
  )
)

# Download file from URL and upload it to S3
iwalk(urls, \(x, idx) {
  file_ext <- paste0(".", file_ext(x))
  remote_file <- file.path(output_bucket, paste0(idx, file_ext))

  if (!aws.s3::object_exists(remote_file)) {
    tmp_file <- tempfile(fileext = file_ext)
    download.file(url = x, destfile = tmp_file)
    save_local_to_s3(remote_file, tmp_file, overwrite = overwrite)
    file.remove(tmp_file)
  }
})
