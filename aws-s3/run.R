library(here)

# Run all scripts
lapply(
  list.files(here("scripts-ccao-landing-us-east-1"), full.names = TRUE),
  function(x) {
    print(paste("Now running:", basename(x)))
    source(x)
  }
)
