library(here)

# Run all raw bucket scripts
lapply(
  list.files(here("scripts-ccao-data-raw-us-east-1"), full.names = TRUE),
  function(x) {
    print(paste("Now running:", basename(x)))
    source(x)
  }
)

# Run all warehouse bucket scripts
lapply(
  list.files(here("scripts-ccao-data-warehouse-us-east-1"), full.names = TRUE),
  function(x) {
    print(paste("Now running:", basename(x)))
    source(x)
  }
)