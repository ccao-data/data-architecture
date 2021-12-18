library(here)
library(purrr)

# Run all raw bucket scripts
walk(
  list.files(here("scripts-ccao-data-raw-us-east-1"), full.names = TRUE),
  function(x) {
    message("Now running:", basename(x))
    source(x)
  }
)

# Run all warehouse bucket scripts
walk(
  list.files(here("scripts-ccao-data-warehouse-us-east-1"), full.names = TRUE),
  function(x) {
    message("Now running:", basename(x))
    source(x)
  }
)