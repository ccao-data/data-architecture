library(odbc)
library(DBI)
library(tidyverse)
library(ccao)
library(stringr)
library(installr)
library(lubridate)


# WRITE ETL FUNCTIONS --

# FUNCTION to Read and process each Trepp file (clean header, clean tails)
read_trepp <- function(f){
  df <- read.csv(f,  header = TRUE, sep = ",", fill = TRUE, skip = 7, check.names = FALSE) %>%
    filter(.[, -which(names(.) == "Property Name")] != "")
  df
}

# Function strip leading and tailing white space
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

# Function clean and process columns
clean_colnames <- function(x) {

  output <- x %>%
    # to lower case
    tolower() %>%

    # # replace " " with "_"
    # gsub(" ", "_", .) %>%

    # replace # with " " and strip leadig and tail whit space
    gsub("\\#", "", .) %>%

    # clean signs: "?"
    gsub("\\?", "", .) %>%

    # clean "%"
    gsub("\\%", "percentage", .) %>%

    # clean "/"
    gsub("\\/", "", .) %>%

    # clean leading and tailing white space
    trim() %>%

    # replace white space to "_"
    gsub(" ", "_", .)


  # return output
  output
}

# FUNCTION to clean each records
sub_replace_signs <- function(val){

  if (!is.na(val)){
    Regex <- "[-]?[0-9]+[.]?[0-9]*|[-]?[0-9]+[L]?|[-]?[0-9]+[.]?[0-9]*[eE][0-9]+"

    # Clean rows: replace "-" with NA
    if( trim(val) == "-" | val == ''){
      val <- NA
    }
    # Clean rows: replace "%" with ""
    if (grepl("%", val, fixed = TRUE)){
      val <- gsub("%", '', val)
    }

    # Clean rows: replace "," with ""
    if (grepl(",", val, fixed = TRUE)){

      val <- gsub(",", '', val)

      # Clean rows: convert number to numerics
      if (grepl(Regex,as.numeric(val))) {
        val <- as.numeric(val)
      }
    }

    # Clean rows: replace *** with NA
    if(grepl("*", val, fixed = TRUE) ) {
      val <- NA
    }

    # Clean rows: convert number to numerics
    if (grepl(Regex,as.numeric(val))) {
      val <- as.numeric(val)
    }

  }

  val
}

# FUNCTION to loop through each column and clean observations
replace_signs <- function(data, columnsList) {

  for(Col in columnsList) {

    # Convert String to Date Type
    if (grepl("date", tolower(Col), fixed = TRUE) ) {
      cleaned <- mdy(data[[Col]])
      data[[Col]]  <-  cleaned

    # Perform General Cleaning
    }else {
      cleaned <- as.vector(sapply(data[[Col]], sub_replace_signs ))
      data[[Col]]  <-  cleaned
      }

  }
  data
}

# FUNCTION to Clean and Process Parcel PIN
clean_parcel <- function(x) {
  if (!is.na(x)) {

    # replace - with ""
    x <- gsub('-', "", x)

    if (nchar(x) == 10) {

      # PIn all 14 digits
      x <- paste0(x, "0000")
    }

  }
  x
}


# LOOP THEN CLEAN & CIMPLILE DATA --
# Read all the file name to loop through later

flst <- list.files('O:/CCAODATA/data/raw_data/trepp_cookcounty', pattern='*.csv', full.names=TRUE)


index <- 1
for (i in 1 : length(flst)) {
  data <- read_trepp(f = flst[i])
  if(index == 1){
    compile <- data
  } else {
    # Concat each looped files on top of each others
    compile <- rbind(compile, data)
  }
  index <- index + 1
}

# Manually formatted (deleted a special character from s string)  nonstandard formatted file(directly downloaded from trepp UI)
df_60618 <- read.csv(text = gsub("\\\\,", "", readLines("../data/small/trepp_cookcounty/nonstanderd_format_files/Market Loan Listing_60618_AfterManualFormat.csv")),
                     header = TRUE,   sep = "," , check.names = FALSE)

# bind df_60618 into compile
compile <- rbind(compile, df_60618)




# ClEAN COMPLIED DATA  --
# create new cleaned column name

all_oldnames <- colnames(compile)
newnames <- as.vector(sapply(list(all_oldnames), clean_colnames))

df_cleaned <- compile %>%
  # rename columns
  rename_at(vars(all_oldnames), ~ newnames) %>%

  # clean the rows under each columns
  replace_signs(data = ., columnsList = newnames) %>%

  # Clean the Parcel PIN
  mutate(assessor_parcel  = as.vector(sapply(.$assessor_parcel, clean_parcel ) ) ) %>%

  # Prettify Parcel PIN
  mutate(assessor_parcel = pin_format_pretty(.$assessor_parcel, full_length = TRUE)) %>%

  # drop observations with no PIN (they can't be matched to assessor data)
  filter(!is.na(assessor_parcel) & !is.na(origination_date)) %>%

  # create a PIN and TAX_YEAR variables for joining
  mutate(PIN = gsub("-", "", assessor_parcel),
         TAX_YEAR = lubridate::year(origination_date))


## INTERITY CHECK --

# Check data row count
paste0(nrow(df_cleaned),
       " Row in trepp data")


# Check Data Columns Count
paste0(length(colnames(df_cleaned)),
       " columns in trepp data")


# Check Duplicates by Assessor Pin
paste0(nrow(df_cleaned %>% dplyr::filter(duplicated(.$assessor_parcel))),
       " trepp assessor ID(s) are not unique in modeling_data but each record potentially contains various value in columns")

# LOAD DATA INTO SQL Server --
# connect to SQL server

CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

if (ask.user.yn.question("Are you certain you want to overwrite TREPPSNAPSHOT?") == TRUE) {

  # replace read credentials with write credentials
  CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATAW"))

  # overwrite table on server
  dbWriteTable(CCAODATA, "TREPPSNAPSHOT", df_cleaned, overwrite = TRUE, row.names = FALSE)

}