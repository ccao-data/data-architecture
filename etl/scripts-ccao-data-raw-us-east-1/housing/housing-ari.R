library(httr)
library(aws.s3)
library(openxlsx)
library(purrr)
source("utils.R")

# This script downloads ARI data from IHDA. ARI data is pulled from census
# indicators and measures housing vulnerability (especially price increases).

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "housing", "ari")

# Define the two known ARI sources
urls <- c(
  "2023" = "https://www.ihda.org/wp-content/uploads/2023/07/2023-ARI.xlsx",
  "2025" = paste0(
    "https://www.ihda.org/wp-content/uploads/2025/08/",
    "Final_ARI_2025.csv"
  )
)

# Download file from URL and upload it to S3
iwalk(urls, \(x, idx) {
  file_ext <- paste0(".", file_ext(x))
  remote_file <- file.path(output_bucket, paste0(idx, file_ext))

  if (!aws.s3::object_exists(remote_file)) {
    tmp_file <- tempfile(fileext = file_ext)
    if (file_ext == ".xlsx") {
      download.file(url = x, destfile = tmp_file)
    } else if (file_ext == ".csv") {
      read_csv(x, skip = 2) %>%
        write.csv(tmp_file)
    }
    save_local_to_s3(remote_file, tmp_file, overwrite = overwrite)
    file.remove(tmp_file)
  }
})
