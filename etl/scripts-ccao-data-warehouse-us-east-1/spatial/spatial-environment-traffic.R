library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(geoarrow)

# Define the S3 bucket and folder path
AWS_S3_RAW_BUCKET <- "s3://ccao-data-raw-us-east-1"
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
s3_folder <- "spatial/environment/traffic/"
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, s3_folder)

# Recoding of road type
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
  "525" = "Over PCC - Rubblized - With No or Partial Reinforcement - But having Hinged Joints",
  "530" = "Over PCC - Rubblized - Full reinforcement",
  "540" = "Over PCC - Rubblized - Continuous reinforcement",
  "550" = "Bituminous Concrete (other than Class I)",
  "560" = "Bituminous Concrete Pavement (Full-Depth)",
  "600" = "Over PCC - Reinforcement unknown",
  "610" = "Over PCC - No reinforcement",
  "615" = "Over PCC - No reinforcement but having short panels and dowels",
  "620" = "Over PCC - Partial reinforcement",
  "625" = "Over PCC - With No or Partial Reinforcement - But having Hinged Joints",
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
  "775" = "Non-Reinforced over PCC - With No or Partial reinforcement but having Hinged Joints",
  "777" = "Reinforced over PCC - With No or Partial reinforcement but having Hinged Joints",
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
      st_transform(4326) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435))

    required_columns <- c(
      "FCNAME", "FC_NAME", "LNS", "SURF_TYP", "SURF_WTH", "SURF_YR", "AADT",
      "CRS_WITH", "CRS_OPP", "CRS_YR", "ROAD_NAME", "DTRESS_WTH", "DTRESS_OPP",
      "SP_LIM", "INVENTORY", "geometry_3435", "year"
    )

    existing_columns <- intersect(required_columns, colnames(shapefile_data))
    shapefile_data <- shapefile_data %>%
      select(all_of(existing_columns)) %>%
      mutate(
        road_type = if ("FCNAME" %in% colnames(.)) FCNAME else if ("FC_NAME" %in% colnames(.)) FC_NAME else NA,
        lanes = if ("LNS" %in% colnames(.)) LNS else NA,
        surface_type = if ("SURF_TYP" %in% colnames(.)) SURF_TYP else NA,
        surface_width = if ("SURF_WTH" %in% colnames(.)) SURF_WTH else NA,
        surface_year = if ("SURF_YR" %in% colnames(.)) SURF_YR else NA,
        daily_traffic = if ("AADT" %in% colnames(.)) AADT else NA,
        condition_with = if ("CRS_WITH" %in% colnames(.)) CRS_WITH else NA,
        condition_opposing = if ("CRS_OPP" %in% colnames(.)) CRS_OPP else NA,
        condition_year = if ("CRS_YR" %in% colnames(.)) CRS_YR else NA,
        road_name = if ("ROAD_NAME" %in% colnames(.)) ROAD_NAME else NA,
        distress_with = if ("DTRESS_WTH" %in% colnames(.)) DTRESS_WTH else NA,
        distress_opposing = if ("DTRESS_OPP" %in% colnames(.)) DTRESS_OPP else NA,
        speed_limit = if ("SP_LIM" %in% colnames(.)) SP_LIM else NA,
        inventory_id = if ("INVENTORY" %in% colnames(.)) INVENTORY else NA
      ) %>%
      mutate(surface_type = road_codes[as.character(surface_type)],
             speed_limit = as.numeric(speed_limit)) %>%
      select(-one_of(required_columns)) %>%
      mutate(across(-geometry, ~replace(., . %in% c(0, "0000"), NA))) %>%
      mutate(surface_year = ifelse(surface_year == 9999, NA, surface_year)) %>%
      group_by(road_name, speed_limit, lanes, surface_type, daily_traffic) %>%
      summarize(geometry = st_union(geometry)) %>%
      ungroup()

    # Function to create the intersection matrix and compute average traffic
    calculate_traffic_averages <- function(shapefile_data) {
      # Create an intersection matrix for averages
      intersection_matrix <- st_intersects(shapefile_data)

      # Create intersecting pairs
      intersecting_pairs <- do.call(rbind, lapply(seq_along(intersection_matrix), function(i) {
        data.frame(polygon_1 = i, polygon_2 = intersection_matrix[[i]])
      })) %>%
        filter(polygon_1 != polygon_2)  # Remove self-matches

      # Add polygon ID and relevant columns to shapefile data. This allows us to later merge
      # data with the intersection pairs above.
      shapefile_with_ids <- shapefile_data %>%
        mutate(polygon_id = row_number()) %>%
        select(polygon_id, road_name, daily_traffic, speed_limit, lanes)

      # Join intersecting pairs with matching street IDs
      averages <- intersecting_pairs %>%
        left_join(
          shapefile_with_ids %>%
            # Create IDs for the "home" street
            rename(
              road_name_1 = road_name,
              daily_traffic_1 = daily_traffic,
              speed_limit_1 = speed_limit,
              lanes_1 = lanes
            ),
          by = c("polygon_1" = "polygon_id")
        ) %>%
        left_join(
          shapefile_with_ids %>%
            # Create IDs for the neighboring streets
            rename(
              road_name_2 = road_name,
              daily_traffic_2 = daily_traffic,
              speed_limit_2 = speed_limit,
              lanes_2 = lanes
            ),
          by = c("polygon_2" = "polygon_id")
        ) %>%
        filter(road_name_1 == road_name_2) %>%  # Keep only matching road names
        group_by(polygon_1) %>%
        # Create averages
        summarize(
          average_daily_traffic = mean(daily_traffic_2, na.rm = TRUE),
          average_speed_limit = mean(speed_limit_2, na.rm = TRUE),
          average_lanes = mean(lanes_2, na.rm = TRUE),
          .groups = 'drop'
        )

      # Update traffic, speed limit, and lanes with averages
      shapefile_data <- shapefile_data %>%
        mutate(polygon_id = row_number()) %>%
        left_join(averages, by = c("polygon_id" = "polygon_1")) %>%
        mutate(
          daily_traffic = if_else(is.na(daily_traffic), average_daily_traffic, daily_traffic),
          speed_limit = if_else(is.na(speed_limit), average_speed_limit, speed_limit),
          num_lanes = if_else(is.na(lanes), average_lanes, lanes)
        )

      return(shapefile_data)
    }


    # Loop until no changes are made
    shapefile_data_final <- shapefile_data
    calculate_traffic_with_loop <- function(shapefile_data) {
      # Initialize final shapefile data
      shapefile_data_final <- shapefile_data

      repeat {
        # Save current values to compare changes
        previous_traffic <- shapefile_data_final$daily_traffic
        previous_speed <- shapefile_data_final$speed_limit
        previous_lanes <- shapefile_data_final$num_lanes

        # Recalculate averages and update shapefile data
        shapefile_data_final <- calculate_traffic_averages(shapefile_data_final)

        # Check if all values remain unchanged
        if (all(previous_traffic == shapefile_data_final$daily_traffic, na.rm = TRUE) &&
            all(previous_speed == shapefile_data_final$speed_limit, na.rm = TRUE) &&
            all(previous_lanes == shapefile_data_final$num_lanes, na.rm = TRUE)) {
          break  # Exit loop if no changes were made
        }
      }

      return(shapefile_data_final)
    }

    calculate_traffic_with_loop(shapefile_data)

    output_path <- file.path(output_bucket, basename(file_key))
    # geoarrow::write_geoparquet(shapefile_data_final, output_path)

    print(paste(file_key, "cleaned and uploaded."))
  }
})

