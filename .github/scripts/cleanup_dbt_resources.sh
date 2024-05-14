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

delete_database() {
    output=$(aws glue delete-database --name "$1" 2>&1)
    status=$?
    if [ "$status" != "0" ]; then
        echo "$output"
        if [[ $output =~ "EntityNotFoundException" ]]; then
            # This case is expected, since it's likely that not all models
            # will be built during CI runs. Suppress the error so
            # that xargs continues executing.
            echo "Continuing execution due to expected 404 response."
        else
            exit 255  # Unexpected error; signal to xargs to quit
        fi
    fi
}
export -f delete_database  # Make delete_database useable by xargs

if [[ "$#" -eq 0 ]]; then
    echo "Missing first argument representing dbt target"
    exit 1
fi

if [ "$1" == "prod" ]; then
    echo "Target cannot be 'prod'"
    exit 1
fi

schemas_json=$(dbt --quiet list --resource-types model seed --target "$1" \
    --exclude config.materialized:ephemeral --output json --output-keys schema \
) || (\
    echo "Error in dbt call" && exit 1
)
schemas=$(echo "$schemas_json"| sort | uniq | jq ' .schema') || (\
    echo "Error in schema parsing" && exit 1
)

echo "Deleting the following schemas from Athena:"
echo
echo "$schemas"

echo "$schemas" | xargs -i bash -c 'delete_database {}'

s3_dependency_dir=$(dbt run-operation print_s3_dependency_dir --quiet \
    --target "$1"
)

echo "Deleting Python model requirements at $s3_dependency_dir"
aws s3 rm "$s3_dependency_dir" --recursive

echo
echo "Done!"
