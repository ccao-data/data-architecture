library(readxl)
library(janitor)
library(dplyr)
library(arrow)
library(aws.s3)
source("utils.R")

s3_dir <- "s3://ccao-data-raw-us-east-1/sale/flag_override/"

objs <- get_bucket(
  bucket = "ccao-data-raw-us-east-1",
  prefix = "sale/flag_override/"
)

tmp_dir <- tempdir()

dfs <- list()

for (obj in objs) {
  key <- obj[["Key"]]
  if (is.null(key) || grepl("/$", key)) next  # skip directory placeholders

  s3_uri <- paste0("s3://ccao-data-raw-us-east-1/", key)

  file_name  <- basename(key)
  local_path <- file.path(tmp_dir, file_name)

  save_s3_to_local(s3_uri, local_path, overwrite = TRUE)

  df <- read_excel(local_path)

  df_name <- make.names(tools::file_path_sans_ext(file_name))
  dfs[[df_name]] <- df
}
# ------------------------------------------------------------------------------
# Transform: one function applied to every dataframe
#   - If name contains "dont_use_char_col", we don't utilize the characteristic
#     change column, since we re-scoped it after realizing excluding minor
#     characteristic changes are too prohitive. We rely on the other columns for
#     this file like flips, class_change, etc.
#   - Otherwise use characteristic_change regex matching strategy to account
#     for human input errors
# ------------------------------------------------------------------------------

add_is_valid_for_modeling <- function(df, use_characteristic_regex = TRUE) {
  df %>%
    janitor::clean_names() %>%
    rename(
      sale_is_arms_length = sale_is_arm_s_length,
      doc_no = sale_doc_no
    ) %>%
    mutate(
      is_valid_for_modeling = !(
        coalesce(sale_is_arms_length == "NO", FALSE) |
          coalesce(flip == "YES", FALSE) |
          coalesce(grepl("YES", class_change, ignore.case = TRUE), FALSE) |
          if (use_characteristic_regex) {
            # Regex statement explained:
            # YES - string match
            # [- ]? - matches a space, a hyphen, and doesn't disqualify omission (YES-MAJOR, YES MAJOR, YESMAJOR)
            # MAJ - string match # [OA] - matches O or A
            # [REOT] - matches R, E, O, or T (we see data like 'YES-MAJOE' and 'YES-MAJOE')
            coalesce(
              grepl("YES[- ]?MAJ[OA][REOT]", characteristic_change, ignore.case = TRUE),
              FALSE
            )
          } else {
            coalesce(grepl("YES", field_check, ignore.case = TRUE), FALSE)
          }
      )
    )
}

dfs_processed <- imap(
  dfs,
  ~ add_is_valid_for_modeling(
    .x,
    use_characteristic_regex = !grepl("dont_use_char_col", .y, ignore.case = TRUE)
  )
)

# Bind all processed dfs together and write to S3 as parquet
data_to_write <- purrr::imap_dfr(
  dfs_processed,
  ~ .x %>% select(doc_no, is_valid_for_modeling),
  .id = "source_file"
) %>%
  select(-source_file)

out_uri <- "s3://ccao-data-warehouse-us-east-1/sale/flag_override/flag_override.parquet" # nolint

tmp_file <- tempfile(fileext = ".parquet")

arrow::write_parquet(
  data_to_write,
  tmp_file,
  compression = "snappy"
)

save_local_to_s3(
  s3_uri = out_uri,
  path = tmp_file,
  overwrite = FALSE
)

unlink(tmp_file)
