#!/usr/bin/env bash
schemas=$(dbt --quiet list --resource-type model --output json --output-keys schema)
echo "Deleting the following schemas from Athena:"
echo
echo "$schemas"

echo "$schemas" \
    | sort \
    | uniq \
    | jq ' .schema' \
    | xargs -i bash -c 'aws glue delete-database --name {}'

echo
echo "Done!"
