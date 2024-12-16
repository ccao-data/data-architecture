library(aws.s3)
library(dplyr)
library(geoarrow)
library(purrr)
library(sf)
library(stringr)

# Define the S3 bucket and folder path
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
s3_folder <- "spatial/environment/road"
output_bucket <- sub("/$", "", file.path(AWS_S3_WAREHOUSE_BUCKET, s3_folder))

# Re-coding of road type
road_codes <- c(
  "010" = "Unimproved",
  "020" = "Graded and Drained",
  "100" = "Without dust palliative treatment",
  "110" = "With dust palliative (oiled)",
  "200" = "Without dust palliative treatment",
  "210" = "With dust palliative treatment",
  "300" = "Bituminous Surface-Treated (low type bituminous)",
  "400" = "Mixed Bituminous (low type bituminous)",
  "410" = "Bituminous Penetration (low type bituminous)",
  "500" = "Bituminous Surface Treated – Mixed bituminous",
  "501" = "Over PCC - Rubblized - Reinforcement unknown",
  "510" = "Over PCC - Rubblized - No reinforcement",
  "520" = "Over PCC - Rubblized - Partial reinforcement",
  "525" = paste(
    "Over PCC - Rubblized - With No or Partial Reinforcement -",
    "But having Hinged Joints"
  ),
  "530" = "Over PCC - Rubblized - Full reinforcement",
  "540" = "Over PCC - Rubblized - Continuous reinforcement",
  "550" = "Bituminous Concrete (other than Class I)",
  "560" = "Bituminous Concrete Pavement (Full-Depth)",
  "600" = "Over PCC - Reinforcement unknown",
  "610" = "Over PCC - No reinforcement",
  "615" = paste(
    "Over PCC - No reinforcement but having short panels",
    "and dowels"
  ),
  "620" = "Over PCC - Partial reinforcement",
  "625" = paste(
    "Over PCC - With No or Partial Reinforcement -",
    "But having Hinged Joints"
  ),
  "630" = "Over PCC - Full reinforcement",
  "640" = "Over PCC - Continuous reinforcement",
  "650" = "Over Brick, Block, Steel, or similar material",
  "700" = "Reinforcement unknown",
  "710" = "No reinforcement",
  "720" = "Partial reinforcement",
  "725" = "With No or Partial reinforcement but having Hinged Joints",
  "730" = "Full reinforcement",
  "740" = "Continuous reinforcement",
  "760" = "Non-Reinforced over PCC - Reinforcement unknown",
  "762" = "Reinforced over PCC - Reinforcement unknown",
  "765" = "Non-Reinforced over PCC - No reinforcement",
  "767" = "Reinforced over PCC - No reinforcement",
  "770" = "Non-Reinforced over PCC - Partial reinforcement",
  "772" = "Reinforced over PCC - Partial reinforcement",
  "775" = paste(
    "Non-Reinforced over PCC - With No or Partial reinforcement",
    "but having Hinged Joints"
  ),
  "777" = paste(
    "Reinforced over PCC - With No or Partial reinforcement",
    "but having Hinged Joints"
  ),
  "780" = "Non-Reinforced over PCC - Full reinforcement",
  "782" = "Reinforced over PCC - Full reinforcement",
  "790" = "Non-Reinforced over PCC - Continuous reinforcement",
  "792" = "Reinforced over PCC - Continuous reinforcement",
  "800" = "Brick, Block or Other"
)

# Get the 'Key'
parquet_files <- get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET, prefix = s3_folder
) %>%
  pull(Key)

# Loop through each parquet file and process it
walk(parquet_files, \(file_key) {
  if (!aws.s3::object_exists(file.path(AWS_S3_WAREHOUSE_BUCKET, file_key))) {
    print(paste("Cleaning", file_key))

    # Convert the S3 object into raw data and read using geoarrow
    shapefile_data <- geoarrow::read_geoparquet_sf(
      file.path(AWS_S3_RAW_BUCKET, file_key)
    ) %>%
      st_transform(4326)

    # Because column names change, we can't just select,
    # but create an intersection of columns we want
    # and the renamed columns.
    required_columns <- c(
      "road_type", "lanes", "surface_type", "surface_width", "surface_year",
      "daily_traffic", "speed_limit", "condition_with", "condition_opposing",
      "condition_year", "road_name", "distress_with", "distress_opposing",
      "inventory", "geometry_3435", "year"
    )

    renames <- c(
      "FCNAME" = "road_type",
      "FC_NAME" = "road_type",
      "LNS" = "lanes",
      "SURF_TYP" = "surface_type",
      "SURF_WTH" = "surface_width",
      "SURF_YR" = "surface_year",
      "AADT" = "daily_traffic",
      "SP_LIM" = "speed_limit",
      "CRS_WITH" = "condition_with",
      "CRS_OPP" = "condition_opposing",
      "CRS_YR" = "condition_year",
      "ROAD_NAME" = "road_name",
      "DTRESS_WTH" = "distress_with",
      "DTRESS_OPP" = "distress_opposing"
    )

    shapefile_data <- shapefile_data %>%
      rename_with(~ str_replace_all(.x, renames))

    missing_columns <- setdiff(required_columns, names(shapefile_data))

    # Add missing columns with NA values directly
    shapefile_data[missing_columns] <- NA

    shapefile_data <- shapefile_data %>%
      select(all_of(required_columns)) %>%
      mutate(
        surface_type = road_codes[as.character(surface_type)],
        speed_limit = as.numeric(speed_limit),
        # For testing
        road_name_preserved = road_name,
        road_name = str_to_lower(road_name), # Convert to lowercase
        road_name = gsub("[[:punct:]]", "", road_name), # Remove punctuation
        # Remove standalone directional indicators (N, S, E, W)
        # I wouldn't remove North South East West, so that streets like North
        # Ave become empty. I also discovered that
        # TH is not universally applied. An example is 100th St.
        # I don't think the added value
        # of removing TH is worth the risk of complicating valid street names.
        # Once again, ending in th would change North Ave.
        road_name = gsub("\\b(n|s|e|w)\\b", "", road_name),

        # Replace full street name words with abbreviations
        road_name = str_replace_all(
          road_name,
          c(
            "\\bavenue\\b" = "ave",
            "\\bav\\b" = "ave",
            "\\bstreet\\b" = "st",
            "\\bcourt\\b" = "ct",
            "\\broad\\b" = "rd",
            "\\bdrive\\b" = "dr",
            "\\bplace\\b" = "pl",
            "\\blane\\b" = "ln",
            "\\btrail\\b" = "trl",
            "\\bparkway\\b" = "pkwy",
            "\\bhighway\\b" = "hwy",
            "\\bexpressway\\b" = "expy"
          )
        ),

        # Remove extra spaces that may result from replacements
        road_name = str_trim(road_name)
      ) %>%
      mutate(across(-geometry, ~ replace(., . %in% c(0, "0000"), NA))) %>%
      mutate(surface_year = ifelse(surface_year == 9999, NA, surface_year)) %>%
      # Group by the characteristics that we want
      group_by(
        road_name, speed_limit, lanes,
        surface_type, daily_traffic, year, road_type
      ) %>%
      # Create a union of the streets based on the summarized features
      summarize(geometry = st_union(geometry), .groups = "drop") %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      ungroup()

    # Helper function to calculate averages based on intersections
    # of streets with the same name
    # and overlapping spatial features.
    calculate_traffic_averages <- function(data) {
      # Create an intersection matrix
      intersection_matrix <- st_intersects(data)

      # Create intersecting pairs
      intersecting_pairs <- map(seq_along(intersection_matrix), \(x) {
        data.frame(
          polygon_1 = x,
          polygon_2 = intersection_matrix[[x]]
        )
      }) %>%
        bind_rows() %>%
        filter(polygon_1 != polygon_2) # Remove self-matches
      # Add polygon IDs and relevant columns for merging
      data_with_ids <- data %>%
        mutate(polygon_id = row_number()) %>%
        select(polygon_id, road_name, daily_traffic, speed_limit, lanes)

      # Join intersecting pairs with their respective polygon data
      averages <- intersecting_pairs %>%
        left_join(
          data_with_ids %>%
            rename(
              road_name_1 = road_name,
              daily_traffic_1 = daily_traffic,
              speed_limit_1 = speed_limit,
              lanes_1 = lanes
            ),
          by = c("polygon_1" = "polygon_id")
        ) %>%
        left_join(
          data_with_ids %>%
            rename(
              road_name_2 = road_name,
              daily_traffic_2 = daily_traffic,
              speed_limit_2 = speed_limit,
              lanes_2 = lanes
            ),
          by = c("polygon_2" = "polygon_id")
        ) %>%
        filter(road_name_1 == road_name_2) %>% # Keep only matching road names
        group_by(polygon_1) %>%
        summarize(
          average_daily_traffic = mean(daily_traffic_2, na.rm = TRUE),
          average_speed_limit = mean(speed_limit_2, na.rm = TRUE),
          average_lanes = mean(lanes_2, na.rm = TRUE),
          .groups = "drop"
        )

      # Update the original data with averages where needed
      shapefile_data_final <- data %>%
        mutate(polygon_id = row_number()) %>%
        left_join(averages, by = c("polygon_id" = "polygon_1")) %>%
        mutate(
          daily_traffic = if_else(is.na(daily_traffic), average_daily_traffic,
            daily_traffic
          ),
          speed_limit = if_else(is.na(speed_limit), average_speed_limit,
            speed_limit
          ),
          lanes = if_else(is.na(lanes), average_lanes, lanes)
        ) %>%
        select(-c(
          average_daily_traffic, average_speed_limit,
          average_lanes, polygon_id
        ))

      return(shapefile_data_final)
    }

    # Run the function
    # Initialize with placeholder to ensure the first iteration runs
    # Initialize previous NA counts with values that
    # differ from any real NA count
    previous_na_counts <- list(
      daily_traffic_na = -1,
      speed_limit_na = -1
    )

    # Loop until there are no changes in NA counts
    while (TRUE) {
      # Calculate current NA counts
      current_na_counts <- list(
        daily_traffic_na = sum(is.na(shapefile_data$daily_traffic)),
        speed_limit_na = sum(is.na(shapefile_data$speed_limit))
      )

      # Check if NA counts have changed
      if (!identical(current_na_counts, previous_na_counts)) {
        print("NA values have changed, recalculating traffic averages.")
        shapefile_data <- calculate_traffic_averages(shapefile_data)

        # Update previous NA counts for the next iteration
        previous_na_counts <- current_na_counts
      } else {
        print("No further NA changes detected, stopping recalculation.")
        break
      }
    }


    shapefile_data <- shapefile_data %>%
      mutate(across(
        -c(geometry, geometry_3435),
        ~ ifelse(is.nan(.), NA, .)
      )) %>%
      select(-year)

    output_path <- file.path(
      output_bucket,
      paste0("year=", tools::file_path_sans_ext(basename(file_key))),
      "part-0.parquet"
      )
    geoparquet_to_s3(shapefile_data, output_path)

    print(paste(file_key, "cleaned and uploaded."))
  }
})
