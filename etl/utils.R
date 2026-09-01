library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(tools)
library(wk)


save_s3_to_local <- function(s3_uri, path, overwrite = FALSE) {
  if (!file.exists(path) || overwrite) {
    message("Saving file: ", s3_uri, " to: ", path)
    aws.s3::save_object(object = s3_uri, file = path)
  }
}


save_local_to_s3 <- function(s3_uri, path, overwrite = FALSE) {
  if (!aws.s3::object_exists(s3_uri) || overwrite) {
    message("Saving file: ", path, "to: ", s3_uri)
    aws.s3::put_object(
      file = path,
      object = s3_uri,
      show_progress = TRUE,
      multipart = TRUE
    )
  }
}


open_data_to_s3 <- function(s3_bucket_uri,
                            base_url,
                            data_url,
                            dir_name,
                            file_year,
                            file_ext,
                            file_prefix = NULL,
                            overwrite = FALSE) {
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


geoparquet_to_s3 <- function(spatial_df, s3_uri, destination) {
  # We are assuming all spatial dataframes passed to this function have a
  # geometry column named "geometry"
  geometry_column <- attr(spatial_df, "sf_column")

  if (destination %in% c("s3_raw", "local")) {
    spatial_df <- spatial_df %>%
      mutate(
        across(where(~ inherits(., "sfc")), as_wkb),
        crs = st_crs(.data[[geometry_column]])$epsg,
        crs_column = geometry_column
      )
  } else if (destination == "s3_warehouse") {
    # This should fail if both the geometry and geometry_3435 columns are not
    # present in the spatial data frame.
    spatial_df <- spatial_df %>%
      mutate(
        temp = as_wkb(geometry),
        geometry_3435 = as_wkb(geometry_3435),
        crs = st_crs(geometry)$epsg,
        loaded_at = as.character(Sys.time())
      ) %>%
      st_drop_geometry() %>%
      rename(geometry = temp) %>%
      relocate(geometry, .before = geometry_3435)
  } else {
    stop(paste(
      "Invalid destination specified.",
      "Must be either 'local', 's3_raw', or 's3_warehouse'."
    ))
  }

  spatial_df[] <- lapply(spatial_df, function(col) {
    if (inherits(col, "wk_wkb")) {
      # Keep only the raw list elements, wipe metadata attributes
      attributes(col) <- NULL
    }
    col
  })

  spatial_df %>%
    as.data.frame() %>%
    write_parquet(., s3_uri, compression = "snappy")
}


write_partitions_to_s3 <- function(df,
                                   s3_output_path,
                                   is_spatial = TRUE,
                                   overwrite = FALSE) {
  if (!dplyr::is.grouped_df(df)) {
    warning("Input data must contain grouping vars for partitioning")
  }

  df <- df %>% mutate(loaded_at = as.character(Sys.time()))
  dplyr::group_walk(df, ~ {
    partitions_df <- purrr::map_dfr(
      .y, tidyr::replace_na, "__HIVE_DEFAULT_PARTITION__"
    )
    partition_path <- paste0(purrr::map2_chr(
      names(partitions_df),
      partitions_df[1, ],
      function(x, y) paste0(x, "=", y)
    ), collapse = "/")
    remote_path <- file.path(
      s3_output_path, partition_path, "part-0.parquet"
    )
    if (!object_exists(remote_path) || overwrite) {
      message("Now uploading: ", partition_path)
      tmp_file <- tempfile(fileext = ".parquet")
      if (is_spatial) {
        geoparquet_to_s3(.x, s3_uri = tmp_file, destination = "s3_warehouse")
      } else {
        write_parquet(.x, tmp_file, compression = "snappy")
      }
      aws.s3::put_object(
        file = tmp_file,
        object = remote_path,
        multipart = TRUE
      )
    }
  })
}


standardize_expand_geo <- function(
  spatial_df, make_valid = FALSE, polygon = TRUE
) {
  return(
    spatial_df %>%
      st_transform(4326) %>%
      {
        if (make_valid) st_make_valid(.) else .
      } %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      {
        if (polygon) {
          mutate(., centroid = st_centroid(st_transform(geometry, 3435))) %>%
            cbind(
              .,
              st_coordinates(st_transform(.$centroid, 4326)),
              st_coordinates(.$centroid)
            ) %>%
            select(!contains("centroid"),
              lon = X, lat = Y, x_3435 = `X.1`, y_3435 = `Y.1`,
              geometry, geometry_3435
            )
        } else {
          select(., dplyr::everything(), geometry, geometry_3435)
        }
      }
  )
}


county_gdb_to_s3 <- function(
  s3_bucket_uri,
  dir_name,
  file_path,
  layer,
  overwrite = FALSE
) {
  remote_file <- file.path(
    s3_bucket_uri,
    dir_name,
    paste0(str_match(file_path, "[0-9]{4}"), ".geojson")
  )

  if (!aws.s3::object_exists(remote_file)) {
    message(paste0("Reading ", basename(file_path)))

    if (layer %in% st_layers(file_path)$name) {
      try({
        tmp_file <- tempfile(fileext = ".geojson")
        st_read(file_path, layer) %>% st_write(tmp_file)
        save_local_to_s3(remote_file, tmp_file, overwrite = overwrite)
        file.remove(tmp_file)
        cat(paste0("File successfully written to ", remote_file, "\n"))
      })
    } else {
      cat(paste0(
        "Layer '", layer,
        "' not present in ",
        basename(file_path),
        "... skipping.\n"
      ))
    }
  }
}


read_s3_geoparquet <- function(s3_uri) {
  # Spatial parquet file must have a geometry column named "geometry" and a
  # CRS column named "crs" for this function to work properly.
  read_parquet(s3_uri) %>%
    st_as_sf(sf_column_name = "geometry") %>%
    st_set_crs(unique(.$crs)) %>%
    select(-crs)
}


collect_s3_geodataset <- function(spatial_dataset) {
  # Spatial dataset must have a geometry column named "geometry" and a CRS
  # column named "crs" for this function to work properly.
  spatial_dataset %>%
    collect() %>%
    st_as_sf(sf_column_name = "geometry") %>%
    st_set_crs(unique(.$crs)) %>%
    select(-crs)
}
