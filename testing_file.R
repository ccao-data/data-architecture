library(DBI)
library(noctua)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(fs)
library(stringr)

# Connect to Athena
con <- dbConnect(noctua::athena())

# Schemas
prod_schema <- "proximity"
dev_schema <- "z_ci_800_fix_geometry_aggregation_rounding_bug_in_dist_to_nearest_geometry_macro_proximity" # nolint: line_length_linter

# Output folder
output_dir <- "differences_by_table"
dir_create(output_dir)

# Get list of matching tables
prod_tables <- dbGetQuery(con, glue("
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = '{prod_schema}' AND table_name LIKE 'dist_%'
"))$table_name

dev_tables <- dbGetQuery(con, glue("
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = '{dev_schema}'
"))$table_name

tables_to_compare <- intersect(prod_tables, dev_tables)

# Loop through each table and compare dist_ft values
walk(tables_to_compare, function(tbl) {
  message("🔍 Comparing table: ", tbl)

  # Read both datasets
  prod_df <- tryCatch(
    dbGetQuery(con, glue("SELECT * FROM {prod_schema}.{tbl}")),
    error = function(e) tibble()
  )

  dev_df <- tryCatch(
    dbGetQuery(con, glue("SELECT * FROM {dev_schema}.{tbl}")),
    error = function(e) tibble()
  )

  if (nrow(prod_df) == 0 || nrow(dev_df) == 0) {
    return(NULL)
  }

  # Identify columns
  dist_cols <- names(prod_df)[str_detect(names(prod_df), "dist_ft$")]
  key_cols <- setdiff(intersect(names(prod_df), names(dev_df)), dist_cols)

  # Join on key columns
  joined <- inner_join(prod_df, dev_df,
    by = key_cols, suffix = c("_prod", "_dev")
  )

  # Compare dist_ft columns
  for (col in dist_cols) {
    prod_col <- paste0(col, "_prod")
    dev_col <- paste0(col, "_dev")
    diff_col <- paste0(col, "_diff")
    joined[[diff_col]] <- joined[[prod_col]] != joined[[dev_col]]
  }

  diff_cols <- paste0(dist_cols, "_diff")

  # Keep rows where any dist_ft column differs
  filtered <- joined %>% filter(if_any(all_of(diff_cols), ~.))

  if (nrow(filtered) == 0) {
    message("✅ No differences for ", tbl)
    return(NULL)
  }

  # Write single CSV per table
  output_path <- file.path(
    output_dir,
    paste0(tbl, "_differences.csv")
  )
  write_csv(filtered, output_path)
  message("💾 Saved differences to ", output_path)
})
