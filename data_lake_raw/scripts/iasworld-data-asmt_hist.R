library(RJDBC)
library(glue)
library(arrow)
library(lubridate)
library(dplyr)
library(here)
library(qpcR)

# Set Rjava option to increase java mem size. Note that this is required for
# large queries!
options(java.parameters = "-Xmx8000m")
options(scipen = 100)

# Establish connection object, the drv parameter points to the locally stored
# Oracle JDBC driver from step 2. You can get it here:
# https://www.oracle.com/database/technologies/appdev/jdbc-ucp-21-1-c-downloads.html
IASWORLD <- RJDBC::dbConnect(
  drv = RJDBC::JDBC(
    driverClass = "oracle.jdbc.OracleDriver",
    classPath = Sys.getenv("DB_CONFIG_IASWORLD_DRV")
  ),
  url = Sys.getenv("DB_CONFIG_IASWORLD")
)

# set batch size: how many rows would you like to pull at a time?
batch_size <- 1000000

# generate row numbers and filepaths for pulls according to batch size
batches <- dbGetQuery(
  IASWORLD,
  "SELECT TAXYR, PROCNAME, COUNT(TAXYR) FROM IASWORLD.ASMT_HIST
  GROUP BY TAXYR, PROCNAME"
  )

batches$file_path = glue("{here('s3-bucket/iasworld/data/ASMT_HIST')}/ASMT_HIST_{batches$TAXYR}_{batches$PROCNAME}.parquet")

# function to pull according to batch size
batch_out <- function(year, proc, file_path) {

  # skip already completed pulls
  if (!file.exists(file_path)) {

    print(
      glue("SELECT * FROM IASWORLD.ASMT_HIST WHERE TAXYR = {year} AND PROCNAME = '{proc}'")
    )

    # record start time
    start <- proc.time()[3]

    # pull
    temp <- dbGetQuery(
      IASWORLD,
      glue(
        "SELECT * FROM IASWORLD.ASMT_HIST
          WHERE TAXYR = {year} AND PROCNAME = '{proc}'"
      )
    )

    # display pull time
    print(
      glue("pull time: {lubridate::seconds_to_period(proc.time()[3] - start)}")
    )

    # write
    write_parquet(temp, file_path)

    rm(temp)
    gc()

  }

}

# apply function
mapply(batch_out, batches$TAXYR, batches$PROCNAME, batches$file_path)

# check row numbers
output_files <- grep(glue_collapse(unique(batches$PROCNAME), "|"), list.files(here('s3-bucket/iasworld/data/ASMT_HIST'), full.names = TRUE), value = TRUE)

total_rows <- 0

for (i in output_files) {

  print(i)
  total_rows <- total_rows + nrow(read_parquet(i))
  gc()

}
