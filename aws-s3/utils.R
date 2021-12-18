library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(sfarrow)
library(tools)


save_s3_to_local <- function(s3_uri, path, overwrite = FALSE) {
  if (!file.exists(path) | overwrite) {
    message("Saving file: ", s3_uri, "to: ", path)
    aws.s3::save_object(object = s3_uri, file = path)
  }
}


save_local_to_s3 <- function(s3_uri, path, overwrite = FALSE) {
  if (!aws.s3::object_exists(s3_uri) | overwrite) {
    message("Saving file: ", path, "to: ", s3_uri)
    aws.s3::put_object(file = path, object = s3_uri)
  }
}


open_data_to_s3 <- function(s3_bucket_uri,
                            base_url,
                            data_url,
                            dir_name,
                            file_year,
                            file_ext,
                            file_prefix = NULL,
                            overwrite = FALSE
                            ) {
  open_data_file <- paste0(base_url, data_url)
  remote_file <- file.path(
    s3_bucket_uri, dir_name,
    gsub(
      "^-", "",
      paste(
        paste(file_prefix, collapse = "-"),
        paste0(file_year, file_ext),
        sep = "-"
      )
    )
  )

  if (!aws.s3::object_exists(remote_file)) {
    tmp_file <- tempfile(fileext = file_ext)
    download.file(url = open_data_file, destfile = tmp_file)
    save_local_to_s3(remote_file, tmp_file, overwrite = overwrite)
    file.remove(tmp_file)
  }
}


write_partitions_to_s3 <- function(df,
                                   s3_output_path,
                                   is_spatial = TRUE,
                                   overwrite = FALSE
                                   ) {
  if (!dplyr::is.grouped_df(df)) {
    warning("Input data must contain grouping vars for partitioning")
  }

  dplyr::group_walk(df, ~ {
    partitions_df <- purrr::map_dfr(
      replace_na(.y, "__HIVE_DEFAULT_PARTITION__")
    )
    partition_path <- paste0(purrr::map2_chr(
      names(partitions_df),
      partitions_df[1, ],
      function(x, y) paste0(x, "=", y)
    ),
    collapse = "/"
    )
    remote_path <- file.path(
      s3_output_path, partition_path, "part-0.parquet"
    )
    if (!object_exists(remote_path) | overwrite) {
      message("Now uploading butts lol: ", partition_path)
      tmp_file <- tempfile(fileext = ".parquet")
      if (is_spatial) {
        sfarrow::st_write_parquet(.x, tmp_file, compression = "snappy")
      } else {
        arrow::write_parquet(.x, tmp_file, compression = "snappy")
      }
      aws.s3::put_object(tmp_file, remote_path)
    }
  })
}