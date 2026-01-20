# ETL

The Data Department uses the ETL pipeline to retrieve and clean all of the
third-party data needed for modeling and reporting.

-  [./scripts-ccao-data-raw-us-east-1](./scripts-ccao-data-raw-us-east-1)
contains scripts that will gather data and upload it to the department's
[raw s3 bucket](https://us-east-1.console.aws.amazon.com/s3/buckets/ccao-data-raw-us-east-1?region=us-east-1&tab=objects)
without any cleaning
-  [./scripts-ccao-data-warehouse-us-east-1](./scripts-ccao-data-warehouse-us-east-1)
contains scripts that will retrieve data from the raw bucket, clean, compile,
and then upload it to the
[warehouse bucket](https://us-east-1.console.aws.amazon.com/s3/buckets/ccao-data-warehouse-us-east-1?region=us-east-1&tab=objects)

This pipeline is generally only used once a year to refresh as much data as
possible prior to modeling. Whoever is responsible for working through the data
refresh should work as much of the raw scripts as they can, then switch over to
running warehouse scripts to clean raw data that has been updated, or warehouse
scripts that don't have raw counterparts. Once that's done
[glue crawlers](https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/crawlers)
can be tiggered to add new data to
[athena](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor).

## Raw Scripts

While working through the raw scripts, keep in mind that many refrence data that
is either static (no longer updated) or public open data that could possibly be
updated, but likely isn't. A good example of the later is Enterprise Zone in
[./scripts-ccao-data-raw-us-east-1/spatial-economy.R](./scripts-ccao-data-raw-us-east-1/spatial-economy.R).
If we search the City of Chicago open data portal for that data asset, we can
see the data hasn't been updated since 2021. While we still needed to check to
make sure our data is as recent as is available, because it already is, we don't
actually have to run that part of the script.

## Warehouse Scripts

Once the raw scripts have been run, these scripts should be run for whichever
raw bucket has new data in it. There are also some warehouse scripts that
operate independent of a raw counterpart ([spatial-census.R](./scripts-ccao-data-warehouse-us-east-1/spatial/spatial-census.R))
and need to be run regardless of progress working through the raw scripts.
Ideally, there shouldn't be any updates that need to be made for these scripts,
but it's possible something about new raw data will cause an error that needs to
be addressed.

# Glue Crawlers

Run the glue crawler for the corresponding warehouse bucket once all the
necessary warehouse scripts for that bucket have been run successfully.
