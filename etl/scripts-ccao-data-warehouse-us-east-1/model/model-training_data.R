# Load libraries ----
library(arrow)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(purrr)
library(stringr)

# Once original data has been uploaded, we should upload the current year
run_year <- format(Sys.Date(), "%Y")

# Connect to Athena
noctua_options(cache_size = 10)
conn <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

# Query final model metadata
metadata <- dbGetQuery(
  conn,
  glue(
    "
    SELECT
      run_id,
      year,
      dvc_md5_assessment_data,
      model_predictor_all_name
    FROM model.metadata
    WHERE run_type = 'final'
    "
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
  dvc_path <- if (as.integer(year) < 2023) {
    glue("s3://ccao-data-dvc-us-east-1/{substr(dvc_hash, 1, 2)}/
         {substr(dvc_hash, 3, 32)}")
  } else {
    glue("s3://ccao-data-dvc-us-east-1/files/md5/{substr(dvc_hash, 1, 2)}
         /{substr(dvc_hash, 3, 32)}")
  }

  message(glue("Processing run_id: {run_id}, year: {year}"))

  # Read and filter training data
  df <- open_dataset(dvc_path) %>%
    select(meta_pin, meta_card_num, all_of(predictor_vars)) %>%
    collect() %>%
    mutate(run_id = run_id)

  # Save to S3, partitioned by run_id
  write_dataset(
    df,
    path = "s3://ccao-data-warehouse-us-east-1/model/training_data",
    format = "parquet",
    partitioning = c("run_id"),
    existing_data_behavior = "overwrite",
    basename_template = paste0(run_id, ".parquet")
  )
}
