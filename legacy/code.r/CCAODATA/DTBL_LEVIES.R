# This code is to
# download, aggregate and write levy info
# (from @fgregg (https://github.com/fgregg/tax_agency_reports))
# from txt file to csv file

library(dplyr)
library(git2r)
library(DBI)
library(odbc)
library(keyring)

CCAODATA <- dbConnect(odbc(), .connection_string = key_get("ccaodata_write", keyring = "ccaodata"))

create_url_list <- function(year) {

  year <- as.character(year)

  # create temporary path
  path <- file.path(tempfile(pattern="levydata-"), "levydata")
  dir.create(path, recursive=TRUE)

  # clone repo using git2r in filepath
  repo <- clone("https://github.com/fgregg/tax_agency_reports/", path)

  # navigate to cloned directory
  setwd(path)

  # grab list of files/folders in directory
  contents <- list.files()

  # navigate to files for desired year if available
  if (year %in% contents) {
    setwd(year)
    agency_text_list <- list.files()
  } else {
    print("Year not found.")
  }

  # create URL list by appending filenames to end of base URL
  tax_url_list <-
    lapply(agency_text_list,
           function(filename)paste0('https://raw.githubusercontent.com/fgregg/tax_agency_reports/master/',
                                    year,
                                    '/',
                                    filename)) %>%
    unlist()

  return(tax_url_list)
}


scrape_tax_info <- function(path){
  data <- read.delim(path, sep='',header = F, stringsAsFactors = F)
  eav <- data[4,9] %>% gsub(',','',.) %>% as.numeric()
  extension <- data[nrow(data)-1, 7]
  # Some txt has
  # *CLERK'S REDUCTION FACTOR 96.8641%
  # so the tax extension and tax rate should be located at the last third row and column 8 and 9

  # Some txt has at the last line
  # 2018 NON CAP FUNDS TAX EXTENSION TOTAL
  # so the tax extension and tax rate should be located at the last third row and column 8 and 9

  # Some txt in 2014 and 2013 has long note

  # Several other scenarios not listed here
  if(extension == ''){
    rate <- data[nrow(data)-2, 9]
    if(rate == ''){
      rate <- data[nrow(data)-2, 8] %>% as.numeric()
      extension <- data[nrow(data)-2, 7]%>% gsub(',','',.) %>% as.numeric()
    } else{
      rate <- rate %>% as.numeric()
      extension <- data[nrow(data)-2, 8]%>% gsub(',','',.) %>% as.numeric()
    }
  } else if(extension == 'TOTAL'){
    extension <- data[nrow(data)-2, 7]
    if(extension == ''){
      extension <- data[nrow(data)-3, 8] %>% gsub(',','',.) %>% as.numeric()
      rate <- data[nrow(data)-3, 9] %>% as.numeric()
    } else{
      extension <- extension %>% gsub(',','',.) %>% as.numeric()
      rate <- data[nrow(data)-2, 8] %>% as.numeric()
    }
  } else if(extension == 'DISTRICT'){
    extension <- data[nrow(data)-3, 7]
    if(extension == ''){
      extension <- data[nrow(data)-4, 8] %>% gsub(',','',.) %>% as.numeric()
      rate <- data[nrow(data)-4, 9] %>% as.numeric()
    } else{
      extension <- extension %>% gsub(',','',.) %>% as.numeric()
      rate <- data[nrow(data)-3, 8] %>% as.numeric()
    }
  } else{
    extension <- extension %>% gsub(',','',.) %>% as.numeric()
    rate <- data[nrow(data)-1, 8] %>% as.numeric()
  }

  agency_code <- data[5,2]
  row <- data.frame(EAV = eav, `Tax Extension` = extension, `Tax Rate` = rate,
                    `Tax Agency Code` = agency_code)
  return(row)
}

aggregate_and_write_tax_info <- function(year, write = F, file_path = NULL){

  # check timing - last try was 6 minutes
  start_time <- Sys.time()

  # generate list of URLs to access file content
  tax_url_list <- create_url_list(year)

  # scrape content from each file
  tmp <- lapply(tax_url_list, scrape_tax_info)

  tmp.table <- do.call('rbind', tmp)
  # check if the conditional sentence includes all scenarios

  # print number of NA values in each column
  print(apply(tmp.table, 2, function(i)mean(is.na(i))))

  if(write==T) {
    write.csv(tmp.table, file = paste0(file_path, as.character(year), '_tax info.csv'))
    end_time <- Sys.time()
    print(end_time - start_time)
  } else {
    end_time <- Sys.time()
    print(end_time - start_time)
    return(tmp.table)
  }
}


############
# aggregate all tax info from year 2006 to year 2018
# match them with the full tax info from 0_digest
# write the fully merged info into one csv file
############



############
# chekc if I scraped all tax agency in year 2018 and merge with other data set

if(F){
  # 977 unique agency codes in 2018 scraped from
  tax_agency_info <- aggregate_and_write_tax_info(2019) # takes more than 2 minutes

  tax_agency_info$Tax.Agency.Code <-
    gsub("-", '', tax_agency_info$Tax.Agency.Code)

  # get all tax agency from cook county clker's website
  clerk_tax <- get_clerk_tax_data(2019)

  # 1513 unique agency code in 2018
  unique_agency_and_name.2018 <- unique(clerk_tax %>% filter(Year==2019) %>% select(`Agency Name`, Agency))

  # merge tax_agency_info and clerk_tax
  full_agency_info <-
    clerk_tax %>%
    filter(Year==2019) %>%
    left_join(tax_agency_info, by = c('Agency' = 'Tax.Agency.Code'))

  # check which tax code is not in the tax_agency_info
  missing_agency <- full_agency_info[which(is.na(full_agency_info$Tax.Rate)),]
  length(unique(missing_agency$Agency)) # 536 missing agency code

  # ramdomly select ten missing info, when querying using the tax agency code,
  # there is no scraped txt file in @fgregg github and
  # no txt file from tax agency report at cook county clerk's website
  sample(unique(missing_agency$Agency), 10)

  # write full_agency_info in csv
  full_agency_info <- full_agency_info %>% mutate(`Calculated.Tax.Rate (100%)` = Tax.Extension/EAV * 100)

  if (ask.user.yn.question("Are you certain you want to overwrite DTBL_LEVIES?") == TRUE) {

    dbWriteTable(full_agency_info, "DTBL_LEVIES", codes, overwrite = TRUE)

  }

}