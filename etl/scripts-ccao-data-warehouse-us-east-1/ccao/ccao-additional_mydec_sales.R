library(arrow)
library(dplyr)
library(openxlsx)

# Declare output paths
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "other", "additional_mydec_sales"
)

input_file <- "O:/CCAODATA/data/additional_mydec_sales/Missing Sales.xlsx"

openxlsx::read.xlsx(input_file, sheet = "Summary") %>%
  select(doc_no = `203.Document.Number`) %>%
  mutate(
    doc_no = gsub("\\D", "", as.character(doc_no)),
    loaded_at = as.character(Sys.time())
  ) %>%
  write_parquet(file.path(output_bucket, "additional_mydec_sales.parquet"))
