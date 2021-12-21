library(arrow)
library(aws.s3)
library(dplyr)
library(ptaxsim)
library(purrr)
library(stringr)
source("utils.R")

# Script to export data from PTAXSIM to S3 for use in reporting
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
