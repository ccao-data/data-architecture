# Uploads a list of document numbers for sales that were recorded in MyDec
# but never made it into iasworld.sales. Used to inject those sales into
# default.vw_pin_sale directly from sale.mydec.
library(arrow)
library(dplyr)
library(openxlsx)

# Declare output paths
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "other", "additional_mydec_sales"
)

# Local copy for now, pending a permanent home on the O: drive
# input_file <- "O:/CCAODATA/data/additional_mydec_sales/Missing Sales.xlsx"
input_file <- "~/repos/data-architecture/Missing Sales.xlsx"

openxlsx::read.xlsx(input_file, sheet = "Summary") %>%
  select(doc_no = `203.Document.Number`) %>%
  distinct() %>%
  mutate(
    doc_no = as.character(doc_no),
    loaded_at = as.character(Sys.time())
  ) %>%
  write_parquet(file.path(output_bucket, "additional_mydec_sales.parquet"))
