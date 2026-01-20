# ETL

The Data Department uses the ETL pipeline to retrieve and clean all of the third-party data needed for modeling and reporting.


-  [./scripts-ccao-data-raw-us-east-1](./scripts-ccao-data-raw-us-east-1) contains scripts that will gather data and upload it to the department's [raw s3 bucket](https://ccao-data-raw-us-east-1.s3.us-east-1.amazonaws.com/) without any cleaning
-  [/scripts-ccao-data-warehouse-us-east-1](/scripts-ccao-data-warehouse-us-east-1) contains scripts that will retrieve data from the raw bucket, clean, compile, and then upload it to the [warehouse bucket](https://ccao-data-warehouse-us-east-1.s3.us-east-1.amazonaws.com/)