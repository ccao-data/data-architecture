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
  # I believe this line is needed because get_bucket
  # also returns prefix objects that end in /
  if (is.null(key) || grepl("/$", key)) next

  s3_uri <- paste0("s3://ccao-data-raw-us-east-1/", key)

  file_name <- basename(key)
  local_path <- file.path(tmp_dir, file_name)

  save_s3_to_local(s3_uri, local_path, overwrite = TRUE)

  df <- read_excel(local_path)

  df_name <- make.names(tools::file_path_sans_ext(file_name))

  df <- df %>% mutate(source_file = df_name)

  dfs[[df_name]] <- df
}

# We have a hard-coded exception for valuations_sale_review_2025.12.16,
# since unlike the rest of the files, we aren't using the
# `characteristic_change` column. In the first round of this collaboration with
# valuations, we weren't distinguishing between minor and major characteristic
# changes. For example, a `YES` in `characteristic_change` could have
# represented a full new floor, or it could have represented a fireplace
# addition. We don't want to exclude all the sales from the training data with
# very minor characteristic changes, so we re-scoped and added a minor/major
# option for this column. As such, for this file specifically we are relying
# on other columns

dfs$valuations_sale_review_2025.12.16 <-
  dfs$valuations_sale_review_2025.12.16 %>%
  janitor::clean_names() %>%
  rename(
    sale_is_arms_length = sale_is_arm_s_length,
    doc_no = sale_doc_no
  ) %>%
  mutate(
    is_arms_length =
      coalesce(sale_is_arms_length == "YES", FALSE),
    is_flip =
      coalesce(flip == "YES", FALSE),
    has_class_change =
      coalesce(
        grepl("YES", class_change, ignore.case = TRUE),
        FALSE
      ),
    has_characteristic_change =
      NA,
    requires_field_check =
      coalesce(
        grepl("YES", field_check, ignore.case = TRUE),
        FALSE
      )
  )

clean_columns <- function(df) {
  df %>%
    janitor::clean_names() %>%
    rename(
      sale_is_arms_length = sale_is_arm_s_length,
      doc_no = sale_doc_no
    ) %>%
    mutate(
      is_arms_length =
        coalesce(sale_is_arms_length == "YES", FALSE),
      is_flip =
        coalesce(flip == "YES", FALSE),
      has_class_change =
        coalesce(
          grepl("YES", class_change, ignore.case = TRUE),
          FALSE
        ),
      has_characteristic_change = case_when(
        grepl(
          "YES[- ]?MAJ[OA][REOT]",
          characteristic_change,
          ignore.case = TRUE
        ) ~ "yes_major",
        grepl(
          "YES[- ]?MIN(?:OR|OT)",
          characteristic_change,
          ignore.case = TRUE
        ) ~ "yes_minor",
        TRUE ~ "no"
      ),
      requires_field_check =
        coalesce(
          grepl("YES", field_check, ignore.case = TRUE),
          FALSE
        )
    )
}

# Keep exception separate from this loop
exception_df <- "valuations_sale_review_2025.12.16"
dfs_processed <- imap(
  dfs[names(dfs) != exception_df],
  ~ clean_columns(.x)
)
dfs_processed[[exception_df]] <- dfs[[exception_df]]

dfs_ready_to_write <- purrr::imap(
  dfs_processed,
  ~ dplyr::select(
    .x,
    doc_no,
    is_arms_length,
    is_flip,
    has_class_change,
    has_characteristic_change,
    requires_field_check,
    source_file
  ) %>%
    mutate(
      loaded_at = Sys.time()
    )
)

# Output dir
out_dir <- "s3://ccao-data-warehouse-us-east-1/sale/flag_override/"

purrr::iwalk(
  dfs_ready_to_write,
  ~ {
    out_uri <- paste0(out_dir, .y, ".parquet")
    tmp_file <- tempfile(fileext = ".parquet")

    arrow::write_parquet(
      .x,
      tmp_file,
      compression = "snappy"
    )

    save_local_to_s3(
      s3_uri = out_uri,
      path = tmp_file,
      overwrite = FALSE
    )

    unlink(tmp_file)
  }
)
