# This script retrieves and uploads a couple of PDFs containing Ohare noise level measurements and addresses of sensors

library(aws.s3)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# file names
files <- c("ORD_Fact_Sheet_Monitors_History.pdf", "ORD_Fact_Sheet_Monitors_Introduction.pdf")


pull_and_write_ohare_noise <- function(x) {

  remote_file <- file.path(
    AWS_S3_RAW_BUCKET, "environment", "ohare_noise", x
  )

  # Print file being written
  print(paste0(Sys.time(), " - ", remote_file))

  # Check to see if file already exists on S3; if it does, skip it
  if (!aws.s3::object_exists(remote_file)) {

    # grab files
    tmp_file <- tempfile(fileext = ".pdf")
    tmp_dir <- tempdir()

    download.file(
      paste0("https://www.oharenoise.org/sitemedia/documents/noise_mitigation/noise_monitors/", x),
      destfile = tmp_file,
      mode = "wb"
    )

    # Write to S3
    aws.s3::put_object(tmp_file, remote_file)
    file.remove(tmp_file)

  }

}

# apply function
lapply(files, pull_and_write_ohare_noise)
