#!/bin/bash

# Script to save crawler configs from Glue to JSON file
aws glue list-crawlers | jq -r .[][] |
while read file; do
    aws glue get-crawler --name ${file} > ${file}.json
done
