library(RJDBC)
library(glue)
library(arrow)
library(lubridate)
library(here)

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
batches <- data.frame(
  "start_row" = seq.int(
  from = 0,
  to = as.numeric(dbGetQuery(IASWORLD, "SELECT NUM_ROWS FROM ALL_TABLES WHERE TABLE_NAME = 'ASMT_HIST'")),
  by = batch_size
))

batches$file_path = glue("{here('s3-bucket/iasworld/data/ASMT_HIST')}/ASMT_HIST_{batches$start_row / batch_size + 1}.parquet")

# function to pull according to batch size
batch_out <- function(start_row, out_path) {

  # output location
  i <- start_row
  file_path <- out_path

  # skip already completed pulls
  if (!file.exists(file_path)) {

    print(
      glue("pulling records {i + 1} through {i + batch_size}")
    )

    # record start time
    start <- proc.time()[3]

    # pull and write
    write_parquet(
      dbGetQuery(
        IASWORLD,
        glue(
          "SELECT * FROM IASWORLD.ASMT_HIST
          OFFSET {i} ROWS
          FETCH NEXT {batch_size} ROWS ONLY"
        )
      ),
      file_path
    )

    # display pull/write time
    print(
      glue("pull/write time: {lubridate::seconds_to_period(proc.time()[3] - start)}")
    )

  }

}

# apply function
mapply(batch_out, batches$start_row, batches$file_path)
