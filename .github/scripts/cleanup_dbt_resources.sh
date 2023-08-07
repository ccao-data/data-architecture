#!/usr/bin/env bash
# Clean up dbt resources created by a CI run or by local development.
#
# Takes one argument representing the target environment to clean up,
# one of `dev` or `ci`. E.g.:
#
# ./cleanup_dbt_resources.sh dev
#
# Assumes that jq is installed and available on the caller's path.
set -euo pipefail

if [[ "$#" -eq 0 ]]; then
    echo "Missing first argument representing dbt target"
    exit 1
fi

if [ "$1" == "prod" ]; then
    echo "Target cannot be 'prod'"
    exit 1
fi

schemas_json=$(dbt --quiet list --resource-type model --target "$1" \
    --output json --output-keys schema) || (echo "Error in dbt call" && exit 1)
schemas=$(echo "$schemas_json"| sort | uniq | jq ' .schema') || (\
    echo "Error in schema parsing" && exit 1
)

echo "Deleting the following schemas from Athena:"
echo
echo "$schemas"

echo "$schemas" | xargs -i bash -c 'aws glue delete-database --name {} || exit 255'

echo
echo "Done!"
