# Load libraries ----
library(arrow)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(purrr)
library(stringr)

# Once original data has been uploaded,
# we should only need to upload the current year data.
run_year <- format(Sys.Date(), "%Y")

# Connect to Athena
noctua_options(cache_size = 10)
conn <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)
AWS_S3_WAREHOUSE_BUCKET <- "s3://ccao-data-warehouse-us-east-1"
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "model", "training_data")

# Query final model metadata
metadata <- dbGetQuery(
  conn,
  glue_sql(
    "
    SELECT
      run_id,
      year,
      dvc_md5_assessment_data,
      model_predictor_all_name
    FROM model.metadata
    WHERE run_type = 'final'
      AND year IN ({run_year*})
    ",
    .con = conn
  )
)

# Iterate through each run
for (i in seq_len(nrow(metadata))) {
  run_id <- metadata$run_id[i]
  year <- metadata$year[i]
  dvc_hash <- metadata$dvc_md5_assessment_data[i]
  predictors_raw <- metadata$model_predictor_all_name[i]

  # Clean predictor names
  predictor_vars <- predictors_raw %>%
    str_remove_all("^\\[|\\]$") %>%
    str_split(",") %>%
    unlist() %>%
    trimws()

  # Build DVC path depending on year
  # The dvc path changes in 2023
  dvc_path <- if (as.integer(year) <= 2023) {
    glue("s3://ccao-data-dvc-us-east-1/{substr(dvc_hash, 1, 2)}/{substr(dvc_hash, 3, 32)}") # nolint: line_length_linter
  } else {
    glue("s3://ccao-data-dvc-us-east-1/files/md5/{substr(dvc_hash, 1, 2)}/{substr(dvc_hash, 3, 32)}") # nolint: line_length_linter
  }

  message(glue("Processing run_id: {run_id}, year: {year}"))

  # Read and filter training data
  df <- open_dataset(dvc_path) %>%
    select(meta_pin, meta_card_num, all_of(predictor_vars)) %>%
    collect()

  # Ensure known type mismatches are cast consistently
  if ("ccao_is_active_exe_homeowner" %in% names(df)) {
    df <- df %>%
      mutate(
        ccao_is_active_exe_homeowner =
          as.logical(ccao_is_active_exe_homeowner)
      )
  }

  # Add run_id after cleaning types
  df <- df %>%
    mutate(run_id = run_id) %>%
    group_by(run_id) %>%
    write_partitions_to_s3(
      output_bucket,
      is_spatial = FALSE,
      overwrite = TRUE
    )
}
