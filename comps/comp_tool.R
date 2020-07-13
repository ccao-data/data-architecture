# License notice ----

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# You should have received a copy of the GNU General Public License along with this program. If not, see https://www.gnu.org/licenses/.

# Top comments ----

# This is an experimental module to find comparable properties (see @97).
# Version 1.0
params <- read.xlsx("ccao_dictionary_draft.xlsx", sheetName = "comp_tool_params", stringsAsFactors = FALSE)
params <- params %>% mutate_at(c("min", "max", "importance"), as.numeric)
comps_folder <- paste0(dirs$data, "comp_tool/")

###Set this for comps save location. Note this will generate ~50+GB of data
folder_to_save <- ("D:/comps/")

#capture universe of sales to compare to using median sale price
get_neighborhood_sales_to_compare <- function(){
  result <- dbGetQuery(
    CCAODATA,
    paste0(
      "WITH local_sales AS (
              SELECT PIN,
              DOC_NO,
              CONVERT(BIGINT, SALE_PRICE) AS SALE_PRICE,
              RECORDED_DATE,
              YEAR(RECORDED_DATE) AS SALE_YEAR,
              CONVERT(INT, SUBSTRING(CONVERT(CHARACTER, HD_TOWN), 1, 2)) AS TOWNSHIP,
              HD_NBHD AS NEIGHBORHOOD
              FROM IDORSALES
              JOIN HEAD ON IDORSALES.PIN = HEAD.HD_PIN
              WHERE SALE_PRICE > 1000 AND YEAR(RECORDED_DATE) >= 2013
              AND TAX_YEAR = 2018 AND HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295)
              AND DEED_TYPE NOT IN ('Q', 'E', 'B')
            )
      SELECT * FROM local_sales LEFT JOIN CCAOSFCHARS AS SF ON local_sales.PIN = SF.PIN
      LEFT JOIN HEAD ON local_sales.PIN = HEAD.HD_PIN AND SF.TAX_YEAR = HEAD.TAX_YEAR + 1
      LEFT JOIN (
      SELECT Name,
      CAST(centroid_x AS FLOAT) as x,
      CAST(centroid_y AS FLOAT) as y,
      PUMA FROM PINLOCATIONS
      ) as geo ON Name = SF.PIN
      WHERE SF.TAX_YEAR = 2018
      "
    )
  )
  result <- result[, !duplicated(colnames(result), fromLast = TRUE)]
  result <- result %>% group_by(PIN) %>% filter(RECORDED_DATE == max(RECORDED_DATE))
  result <- result %>% mutate(PUMA = as.numeric(PUMA),
                              Year = year(RECORDED_DATE),
                              Quarter = quarter(RECORDED_DATE),
                              SALE_PRICE = as.numeric(SALE_PRICE))
  
  result <- IHS_sale_time_adj(result, 2018, 1)
  result <- result %>% mutate(ADJ_SALE_PRICE = as.integer(SALE_PRICE * adjustment_factor))
  
  by_town_neigh <- group_by(result, TOWNSHIP, NEIGHBORHOOD)
  sales_by_neighborhood <- summarise(by_town_neigh, num_sales = n(), med_sale = median(ADJ_SALE_PRICE), avg_sale = mean(ADJ_SALE_PRICE))
  return(list(as.data.table(result), sales_by_neighborhood))
}


#get universe of sales to compare to by drawing a circle around a pin location
get_pin_and_neighbors <- function(pin, neighborhood_size) {
  pin_info <- get_pin_info(pin)
  
  #filter for case when most recent sale includes multiple sales
  # .019444 ~ 1 mile longitude (E/W) centroid_X
  # .014444 ~ 1 mile latitude (N/S) centroid_y
  
  #constants for SQL pull
  x_dist <- 0.019444 * neighborhood_size
  y_dist <- 0.014444 * neighborhood_size
  
  neighbor_info <- as.data.frame(sales_universe[[1]])
  neighbor_info <- neighbor_info %>% filter(x > pin_info$x - x_dist, x < pin_info$x + x_dist, y > pin_info$y - y_dist, y < pin_info$y + y_dist)
  
  if (nrow(neighbor_info) > 0){
    
    #change date format
    neighbor_info["SALE_YEAR"] <- format(as.Date(neighbor_info$RECORDED_DATE), "%Y")
    
    #filter sales with too low price; filter to most recent sales
    neighbor_info["SALE_PRICE"] <- lapply(neighbor_info["SALE_PRICE"], as.integer)
    neighbor_info <- neighbor_info %>% filter(SALE_PRICE > 100)
    neighbor_info <- neighbor_info %>% group_by(PIN) %>% filter(RECORDED_DATE == max(RECORDED_DATE))
    
    #calculate distance in feet; probably slightly buggy
    neighbor_info["DISTANCE"] <-
      distVincentyEllipsoid(cbind(pin_info$x, pin_info$y),
                            subset(neighbor_info, select = c(x, y))) * 3.28084
  }
  return(list(pin_info, neighbor_info))
}

#stats on pin
get_pin_info_universe <- function(){
  result <- dbGetQuery(
    CCAODATA,
    paste0(
      "SELECT *
        FROM CCAOSFCHARS as SF
        LEFT JOIN HEAD ON SF.PIN = HEAD.HD_PIN AND SF.TAX_YEAR = HEAD.TAX_YEAR + 1
        LEFT JOIN (SELECT Name, CAST(centroid_x AS FLOAT) as x, CAST(centroid_y AS FLOAT) as y, PUMA FROM PINLOCATIONS) as geo ON Name = SF.PIN
        WHERE SF.PIN IN (SELECT PIN FROM HEAD WHERE TAX_YEAR = 2018 AND HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295) AND HD_ASS_LND + HD_ASS_BLD > 100) AND SF.TAX_YEAR = 2018"
    )
  )
  result <- result[, !duplicated(names(result), fromLast=TRUE)]
  result <- result[complete.cases(result), ]
  result <- as.data.table(result)
  setkeyv(result, cols=c("HD_PIN"))
  result <- subset(unique(result))
  
  result <- result %>% mutate(PUMA = as.numeric(PUMA),
                              Year = TAX_YEAR,
                              Quarter = 1,
                              AV = 10 * (HD_ASS_BLD + HD_ASS_LND))
  
  result <- IHS_sale_time_adj(result, 2018, 1)
  result <- result %>% mutate(AV = AV * adjustment_factor)
  result <- as.data.table(result)
  
  return(result)
}

mini_percentile_method <- function(universe, params, pin_info){
  new <- params[params["method"] == "percentiles", ]
  for (col in new$Attribute) {
    if (nrow(universe) > 0){
      universe <- universe[get(col) >= pin_info[, get(col)] * new[new["Attribute"] == col, ]["min"][[1]] &
                             get(col) <= pin_info[, get(col)] * new[new["Attribute"] == col, ]["max"][[1]]]
    }
  }
  return(universe)
}

best_method_only <- function(pin_info){
  pin_info <- as.data.table(pin_info)
  town_nbhd_sales <- sales_universe[[1]]
  town_nbhd_sales <- town_nbhd_sales[HD_TOWN == pin_info$HD_TOWN & HD_NBHD == pin_info$HD_NBHD, ]
  percentile_comps <- mini_percentile_method(town_nbhd_sales, params, pin_info)
  if (nrow(percentile_comps) > 0){
    comp_df <- percentile_comps %>% dplyr::select(SALE_PRICE, RECORDED_DATE, PIN) %>% 
      rename(COMP_PIN = PIN,
             COMP_SALE = SALE_PRICE,
             SALE_DATE = RECORDED_DATE) %>%
      mutate(TARGET_PIN = pin_info$HD_PIN,
             METHOD = "1")
    return(comp_df)
  }
  return(data.frame())
}

secondary_method <- function(pin_info){
  pin_info <- as.data.table(pin_info)
  pin_AV <- pin_info$AV
  
  filter_med_sale_percent <- 0.05
  comp_neighborhoods <-
    sales_universe[[2]] %>% filter(med_sale >= pin_AV * (1 - filter_med_sale_percent)) %>%
    filter(med_sale <= pin_AV * (1 + filter_med_sale_percent))
  
  while (sum(comp_neighborhoods$num_sales) < 1000){
    filter_med_sale_percent <- filter_med_sale_percent * 1.01
    comp_neighborhoods <-
      sales_universe[[2]] %>% filter(med_sale >= pin_AV * (1 - filter_med_sale_percent)) %>%
      filter(med_sale <= pin_AV * (1 + filter_med_sale_percent))
    if (filter_med_sale_percent > 0.50){
      break
    }
  }
  medsale_universe <- as.data.table(inner_join(sales_universe[[1]], comp_neighborhoods, by = c("NEIGHBORHOOD", "TOWNSHIP")))
  percentile_comps <- mini_percentile_method(medsale_universe, params, pin_info)
  if (nrow(percentile_comps) > 0){
    comp_df <- percentile_comps %>% dplyr::select(SALE_PRICE, RECORDED_DATE, PIN) %>% 
      rename(COMP_PIN = PIN,
             COMP_SALE = SALE_PRICE,
             SALE_DATE = RECORDED_DATE) %>%
      mutate(TARGET_PIN = pin_info$HD_PIN,
             METHOD = "2")
    return(comp_df)
  }
}

build_comps_lst <- function(){
  result <- dbGetQuery(
    CCAODATA,
    paste0("SELECT HD_PIN, PROP_ADDRESS FROM HEAD
          LEFT JOIN
          	 (SELECT
          		PL_PIN,
          	  ((REPLACE(CAST(PL_HOUSE_NO AS VARCHAR), ' ', '')) + ' ' +
          	  (REPLACE(CAST(PL_DIR AS VARCHAR), ' ', '')) + ' ' +
          	  (REPLACE(CAST(PL_STR_NAME AS VARCHAR), ' ', '')) + ' ' +
          	  (REPLACE(CAST(PL_STR_SUFFIX AS VARCHAR), ' ', '')) + ', ' +
          	  (REPLACE(CAST(PL_CITY_NAME AS VARCHAR), ' ', '')) + ' ' +
          	  (REPLACE(CAST(PL_ZIPCODE AS VARCHAR), ' ', '')) + ', ' +
          	  (REPLACE(CAST(PL_APT_NO AS VARCHAR), ' ', ''))) AS PROP_ADDRESS FROM PROPLOCS) AS PLC
          	  ON HEAD.HD_PIN = PLC.PL_PIN
          WHERE TAX_YEAR = 2018 AND HD_CLASS IN (202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295) AND HD_ASS_LND + HD_ASS_BLD > 100
          "))
  result <- result[order(result$HD_PIN), ]
  file_name <- paste0(comps_folder, "master_comps_ls.csv")
  write.table(result, file=file_name, row.names=FALSE, sep="|")
}

build_comps_db <- function(block_size, output_name, start_ind, end_ind, method){
  print(output_name)
  comps_to_write <- data.frame()
  indx_cnt <- nrow(pin_info_universe)
  pointer <- start_ind
  end_ind <- min(indx_cnt, end_ind)
  print(method)
  while (pointer < end_ind){
    print(pointer)
    target_pins <- master_ls[c(pointer : (pointer + block_size - 1)), "HD_PIN"]
    mini_info <- pin_info_universe[pin_info_universe$PIN %in% target_pins, ]
    if (method == 1){
      comps_results <- try(mini_info[, best_method_only(c(.BY, .SD)), by = seq_len(nrow(mini_info))])
    } else {
      comps_results <- try(mini_info[, secondary_method(c(.BY, .SD)), by = seq_len(nrow(mini_info))])
    }
    
    if (class(comps_results) != "try-error"){
      comps_to_write <- rbindlist(list(comps_to_write, as.data.frame(comps_results)))
    }
    if (nrow(comps_to_write) > 1000){
      write.table(comps_to_write, file=paste0(folder_to_save, output_name), row.names=FALSE, col.names=FALSE, append=TRUE)
      comps_to_write <- data.frame()
    }
    pointer <- pointer + block_size
  }
}

workflow_manager <- function(position){
  assignmentlist <- read.xlsx(paste0(comps_folder, "compsassignment.xlsx"), sheetName = 'Sheet1')
  work_to_do <- assignmentlist %>% filter(Assignee == position)
  
  for (entry in 1:nrow(work_to_do)){
    job <- work_to_do[entry, ]
    try(build_comps_db(1000, as.character(job["Filename"][[1]]), job["Start"][[1]], job["End"][[1]], job["Method"][[1]]), master_ls)
  }
}

#global sales
sales_universe <- get_neighborhood_sales_to_compare()
pin_info_universe <- get_pin_info_universe()
if (!file.exists(paste0(comps_folder, "master_comps_ls.csv"))){
  build_comps_lst()
}
master_ls <- read.table(paste0(comps_folder, "master_comps_ls.csv"), header=TRUE, sep="|", colClasses =  "character")

#verify comps generated for every pin
update_comps_ls <- function(){
  
  assignmentlist <- read.xlsx(paste0(comps_folder,'compsassignment.xlsx'), sheetName = 'Sheet1')
  unique_files <- unique(assignmentlist$Filename)
  master_ls <- read.table(paste0(comps_folder, 'master_comps_ls.csv'), header=TRUE, sep="|", colClasses =  "character")
  master_ls <- master_ls %>% mutate(PROP_ADDRESS = str_squish(PROP_ADDRESS))
  
  for (i in 1:length(unique_files)){
    print(i)
    cur_row <- assignmentlist[i, ]
    temp_file <- fread(paste0(folder_to_save, as.character(cur_row["Filename"][[1]])), colClasses="character")
    names(temp_file) <- c("num", "COMP_SALE", "SALE_DATE", "COMP_PIN", "TARGET_PIN", "METHOD")
    temp_file[, c("num"):=NULL]
    temp_file <- temp_file %>% distinct()
    temp_file <- temp_file %>% left_join(master_ls, by = c("TARGET_PIN" = "HD_PIN")) %>% 
      rename(TARGET_PIN_ADDRESS = PROP_ADDRESS) %>% 
      left_join(master_ls, by = c("COMP_PIN" = "HD_PIN")) %>% 
      rename(COMP_PIN_ADDRESS = PROP_ADDRESS)
    temp_file <- temp_file %>% left_join(sales_universe[[1]][,c("PIN", "ADJ_SALE_PRICE")], by = c("COMP_PIN" = "PIN"))
    fwrite(temp_file, file=paste0("D:/to_upload/", as.character(cur_row["Filename"][[1]])), row.names=FALSE, col.names=FALSE, append=TRUE)
  }
}

get_comps_density <- function(){
  assignmentlist <- read.xlsx(paste0(comps_folder,'compsassignment.xlsx'), sheetName = 'Sheet1')
  master_ls <- read.table(paste0(comps_folder, 'master_comps_ls.csv'), header=TRUE, sep="|", colClasses =  "character")
  
  master_ls["method_1_comps"] <- 0
  master_ls["method_2_comps"] <- 0
  
  for (file in unique(assignmentlist$Output_File)){
    big_tbl <- fread(paste0(folder_to_save, file), colClasses="character")
    names(big_tbl) <- c("COMP_SALE", "SALE_DATE", "COMP_PIN", "TARGET_PIN", "METHOD")
    setkeyv(big_tbl, cols=c("COMP_SALE", "SALE_DATE", "COMP_PIN", "TARGET_PIN", "METHOD"))
    big_tbl <- subset(unique(big_tbl))
    fwrite(big_tbl, paste0(folder_to_save, file), row.names=FALSE, col.names=FALSE)
    
    pins_and_counts <- big_tbl %>% group_by(TARGET_PIN, METHOD) %>% summarise(cnt = n())
    
    master_ls <- master_ls %>% left_join(pins_and_counts %>% filter(METHOD == 1), by=c("HD_PIN" = "TARGET_PIN")) %>% 
      mutate(method_1_comps = ifelse(is.na(cnt), method_1_comps, method_1_comps + ifelse(METHOD == 1, cnt, 0))) %>% dplyr::select(-cnt, -METHOD)
    
    master_ls <- master_ls %>% left_join(pins_and_counts %>% filter(METHOD == 2), by=c("HD_PIN" = "TARGET_PIN")) %>% 
      mutate(method_2_comps = ifelse(is.na(cnt), method_2_comps, method_2_comps + ifelse(METHOD == 2, cnt, 0))) %>% dplyr::select(-cnt, -METHOD)
  }
  
  fwrite(master_ls, file=paste0(comps_folder, 'master_comps_ls_with_cnts.csv'), row.names=FALSE, sep="|")
}

reset_comps_sql_tbl <- function(verify){
  if (verify){
    dbRemoveTable(CCAODATA, "COMPS")
    db_col_names <- c("varchar(10)", "varchar(10)", "varchar(14)", "varchar(14)", "varchar(2)", "varchar(75)", "varchar(75)", "varchar(10)")
    names(db_col_names) <- c("SALE_PRICE", "SALE_DATE", "COMP_PIN", "TARGET_PIN", "COMP_METHOD", "TARGET_ADDRESS", "COMP_ADDRESS", "ADJ_SALE_PRICE")
    dbCreateTable(CCAODATA, "COMPS", db_col_names)
  }
}


build_sql_db <- function(){
  assignmentlist <- read.xlsx(paste0(comps_folder,'compsassignment.xlsx'), sheetName = 'Sheet1')
  unique_files <- unique(assignmentlist$Filename)
  
  for (i in 1:length(unique_files)){
    print(i)
    cur_row <- assignmentlist[i, ]
    tmp <- fread(paste0("D:/to_upload/", as.character(cur_row["Filename"][[1]])), colClasses="character")
    names(tmp) <- c("SALE_PRICE", "SALE_DATE", "COMP_PIN", "TARGET_PIN", "COMP_METHOD", "TARGET_ADDRESS", "COMP_ADDRESS", "ADJ_SALE_PRICE")
    print(paste0("writing ", i))
    dbWriteTable(CCAODATA, "COMPS", tmp, append = TRUE, row.names=FALSE)
  }
}



