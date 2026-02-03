# This script ingests valuation analyst review workbooks and
# normalizes several yes-no columns into standardized flags
# (arms-length, flip, class change, characteristic change, field check)
#
# The parsing logic relies on string equality and regex matching against
# heterogeneous analyst inputs, so before running on new workbooks we
# should spot-check new values/typos (variants of "YES", "YES-MAJOR",
# "YES MINOR", etc) to ensure edge cases are still classified correctly

library(arrow)
library(aws.s3)
library(dplyr)
library(janitor)
library(readxl)

source("utils.R")

objs <- get_bucket(
  bucket = "ccao-data-raw-us-east-1",
  prefix = "sale/flag_review/"
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

  df <- read_excel(local_path, guess_max = 20000)

  df_name <- make.names(tools::file_path_sans_ext(file_name))

  df <- df %>% mutate(source_file = df_name)

  dfs[[df_name]] <- df
}

# This is a duplicate, we prefer the other determination that Jon made from
# file valuations_sale_review_2025.12.16
dfs$valuations_sale_review_01_30_2026_diane <-
  dfs$valuations_sale_review_01_30_2026_diane %>%
  filter(`Sale Doc No` != "2516802041")

clean_columns_and_whitespace <- function(df) {
  df %>%
    janitor::clean_names() %>%
    rename(
      sale_is_arms_length = sale_is_arm_s_length,
      doc_no              = sale_doc_no
    ) %>%
    # Finds any column with both "work" and "drawer"
    # and renames to work_drawer
    rename_with(
      .fn = ~"work_drawer",
      .cols = matches("work.*drawer|drawer.*work")
    ) %>%
    rename_with(
      .fn   = ~"flip",
      .cols = matches("flip")
    ) %>%
    mutate(across(where(is.character), stringr::str_trim))
}

drop_rows_missing_all_review_flags <- function(df) {
  # With many excel files, particularly of the data received
  # on 01_30_2026, we are sent multiple excel files that contain ALL
  # of the south tri sales for 2025. But this work was split out for many
  # different reviewers. As such, only a small subset of all of the sales
  # are actually reviewed. To properly capture which sales have gotten a
  # review, this function excludes doc_no's where there is no value for any
  # of the key review fields.
  cols <- c(
    "sale_is_arms_length",
    "characteristic_change",
    "class_change",
    "flip"
  )

  # Only apply if the columns exist (they should, post-cleaning)
  df %>%
    filter(
      !if_all(
        all_of(cols),
        ~ is.na(.x) | .x == ""
      )
    )
}


# Check to make sure we have properly standardized column names
# for the manual exception of `valuations_sale_review_2025.12.16`
# and for the `transform_columns()` function loop
required_raw_cols <- c(
  "sale_is_arms_length",
  "flip",
  "class_change",
  "characteristic_change",
  "field_check",
  "work_drawer"
)

assert_required_cols <- function(df, df_name = "<unnamed>") {
  missing <- setdiff(required_raw_cols, names(df))
  if (length(missing) > 0) {
    stop(
      sprintf(
        "Missing required raw column(s) in %s: %s",
        df_name,
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  df
}

transform_columns <- function(df) {
  df %>%
    clean_columns_and_whitespace() %>%
    drop_rows_missing_all_review_flags() %>%
    assert_required_cols(df_name) %>%
    mutate( # nolint
      is_arms_length =
        coalesce(
          grepl("YES", sale_is_arms_length, ignore.case = TRUE),
          FALSE
        ),
      is_flip = coalesce(
        grepl("YES", flip, ignore.case = TRUE),
        FALSE
      ),
      has_class_change =
        coalesce(
          grepl("YES", class_change, ignore.case = TRUE),
          FALSE
        ),
      has_characteristic_change = case_when(
        # Regex statement explained:
        # - YES - string match
        # - [^A-Z]* - matches zero or more non-letter characters between YES
        #   and MAJ/MIN (captures separators like ":", "-", spaces, "/",
        #   parentheses, em dashes, etc.,
        #   while still allowing omission: YESMAJOR / YESMINOR)
        # - MAJ - string match
        # - [OA] - matches O or A
        # - [REOT] - matches R, E, O, or T (we see typos like 'YES-MAJOE'
        #   and 'YES-MAJOT')
        # - MIN - string match
        # - (?:OR|OT) - matches OR or OT (we see typos like 'YES-MINOT')
        grepl(
          "YES[^A-Z]*MAJ[OA][REOT]",
          characteristic_change,
          ignore.case = TRUE
        ) ~ "yes_major",
        grepl(
          "YES[^A-Z]*MIN(?:OR|OT)",
          characteristic_change,
          ignore.case = TRUE
        ) ~ "yes_minor",
        TRUE ~ "no"
      ),
      requires_field_check =
        coalesce(
          grepl("YES", field_check, ignore.case = TRUE),
          FALSE
        ),
      work_drawer =
        coalesce(
          grepl("YES", work_drawer, ignore.case = TRUE),
          FALSE
        )
    )
}

# Make a small modification to adjust for differing input.
# This DF holds a large amount of char change data 'YES', which doesn't fit
# into our schema, which we will ignore. There is some data that just holds
# 'MAJOR' or 'MINOR' type responses, which we want to capture.

# Instead of building this into the main function, we make the edit here,
# since it is currently a small edge case of data.
dfs$valuations_sale_review_01_30_2026_peter <-
  dfs$valuations_sale_review_01_30_2026_peter %>%
  mutate(
    `Characteristic Change` = case_when(
      grepl(
        "\\bmajor\\b", `Characteristic Change`,
        ignore.case = TRUE
      ) ~ "yes_major",
      grepl(
        "\\bminor\\b", `Characteristic Change`,
        ignore.case = TRUE
      ) ~ "yes_minor",
      grepl(
        "\\bno\\b", `Characteristic Change`,
        ignore.case = TRUE
      ) ~ "no",
      is.na(`Characteristic Change`) ~ NA_character_,
      TRUE ~ "no"
    )
  )

dfs_processed <- purrr::imap(
  dfs,
  ~ transform_columns(.x)
)

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
    work_drawer,
    source_file
  ) %>%
    mutate(
      loaded_at = as.character(Sys.time())
    )
)

# Output dir
out_dir <- "s3://ccao-data-warehouse-us-east-1/sale/flag_review/"

purrr::iwalk(
  dfs_ready_to_write,
  ~ {
    arrow::write_parquet(
      .x,
      sink = paste0(out_dir, .y, ".parquet"),
      compression = "snappy"
    )
  }
)
