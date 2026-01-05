library(aws.s3)
library(dplyr)
library(readxl)
library(tools)
library(writexl)

# Source directory with Excel files, provided by valuations
src_dir <- "O:/CCAODATA/data/sale"

# Output dir
s3_dir <- "s3://ccao-data-raw-us-east-1/sale/flag_override/"

# List Excel files
excel_files <- list.files(
  path = src_dir,
  pattern = "\\.xlsx$",
  full.names = TRUE
)

# Read each Excel file and write back to S3 as Excel
for (f in excel_files) {
  base_name <- file_path_sans_ext(basename(f))
  s3_path <- paste0(s3_dir, base_name, ".xlsx")

  df <- read_excel(f)

  s3write_using(
    x = df,
    FUN = write_xlsx,
    object = s3_path
  )
}
