#!/usr/bin/env bash
# Parse, install, zip, and upload dbt Python model requirements to S3.
#
# This script checks for Python model dependencies by searching every
# model node for a `config.packages` attribute and uploading a zip archive
# for every version of every package that is specified in one or more such
# attributes. It expects attribute values to be well-formed Python requirement
# specifiers that are pinned to a specific version of the package, e.g.
# `assesspy==1.0.1` is allowed but not `assesspy==1.0.*` or `assesspy~=1.0.1`.
#
# If a zip archive for a given version of a dependency is already present in
# the remote dependency dir on S3, the script will skip installing, zipping,
# and uploading that dependency version.
#
# When uploading dependencies, package names get cleaned in order to make them
# fit the required format for importable Python identifiers. This process
# involves the following transformations:
#
# * Hyphens ("-") and periods (".") get replaced with underscores ("_")
# * Double equals signs ("==") get replaced with the version prefix "_v"
#
# For example, the string "assesspy==1.0.1" will get saved to a zip archive
# called "assesspy_v1_0_1". This string can then be used to import the package
# into a Python model script with a set of import calls like so:
#
#   sc.addPyFile(f"{s3_dependency_dir}/assesspy_v1_1_0.zip")
#   import assesspy_v1_1_0 as assesspy
#
# Assumes that dbt, python, and jq are installed and available on the caller's
# path. Also assumes that it is being run in a directory containing a
# dbt project.
#
# Example usage:
#
#   ../.github/scripts/deploy_dbt_model_dependencies.sh
set -euo pipefail

# Fall back to local development if a dbt $TARGET is not specified
target=${TARGET:-"dev"}
echo "Parsing dbt Python model dependencies for target '$target'"

# Determine the remote location where bundled dependencies will be deployed
s3_dependency_dir="s3://ccao-athena-dependencies-us-east-1"

# Compile the DAG so that we have up-to-date info on dependencies
dbt compile --quiet --target "$target"

# Extract the config.packages attribute from only the models
# where it is set
packages_json=$(jq '
    .nodes |
        map(
            select(
                .config.packages != null and (.config.packages | length) > 0
            ) |
            .config.packages
        ) |
        flatten |
        unique
' target/manifest.json)

echo "Extracted the following requirements from the dbt DAG:"
echo "$packages_json"

# Convert the JSON array to a bash array
mapfile -t packages_array < <(echo "$packages_json" | jq -r '.[]')

# Before moving forward, validate the package names to ensure that they
# match our requirements for deterministic installation. We want to do this as
# soon as possible to quickly return to the user in case of a formatting error
for package_name in "${packages_array[@]}"; do
    if [[ "$package_name" != *'=='* ]] || [[ "$package_name" == *'*'* ]]; then
        echo "Error: Requirement identifier '${package_name}' must include '==' and must not include '*'."
        echo "This is necessary to ensure that dependency installs are deterministic."
        echo "Please edit the config.packages attribute containing this string and try again."
        exit 1
    fi
done

# Set a flag to check whether any dependencies were found so we can log
# a warning if none are found
new_dependencies_found=false

# Define the name of the virtualenv and set a flag to record whether it
# has been created
venv_name="deploy-dbt-model-dependencies-venv"
venv_created=false

# Iterate over each key-value pair representing a set of package
# dependencies and output those dependencies to a requirements file
for package_name in "${packages_array[@]}"; do
    # Set the flag to confirm new dependencies were found, since if we reach
    # this point it means that packages_array is non-empty
    new_dependencies_found=true

    # Transform the package name to fit the requirements of Python imports
    cleaned_package_name=$(\
        echo "$package_name" | \
            sed -e 's/-/_/g' -e 's/\./_/g' -e 's/==/_v/g' \
    )
    zip_archive_name="${cleaned_package_name}.zip"

    # Check to see if the package already exists in S3
    existing_package_url="${s3_dependency_dir}/${zip_archive_name}"
    if aws s3 ls "$existing_package_url" > /dev/null 2>&1; then
        echo "$cleaned_package_name already exists at ${existing_package_url}, skipping upload"
        continue
    fi

    # Create and activate a Python virtual environment for installing
    # dependencies. Only do this once we know that we have at least one
    # dependency to upload, since virtualenv creation takes a few seconds
    if [ "$venv_created" == "false" ]; then
        echo "Creating and activating virtualenv at $venv_name"
        python3 -m venv "$venv_name"
        # Stop shellcheck from getting mad that the virtualenv dir doesn't exist at
        # compile time. It's not a problem since the Python `venv` command will
        # take care of creating it
        # shellcheck disable=SC1091
        source "${venv_name}/bin/activate"
        venv_created=true
    fi

    # Install dependencies into a subdirectory that we can use for bundling
    subdirectory_name="${cleaned_package_name}/"
    mkdir -p "$subdirectory_name"
    echo "Installing '${package_name}' into $subdirectory_name"
    pip install -t "$subdirectory_name" "$package_name" --no-deps

    # Create a zip archive from the contents of the subdirectory
    echo "Creating zip archive $zip_archive_name from $subdirectory_name"
    cd "$subdirectory_name" && zip -q -r9 "../$zip_archive_name" ./* && cd ..

    # Upload the zip archive and the requirements file to S3
    echo "Uploading $zip_archive_name to S3"
    aws s3 cp "$zip_archive_name" "${s3_dependency_dir}/" --no-progress

    # Clean up intermediate artifacts
    echo "Cleaning up package directory and zip archive"
    rm -rf "$subdirectory_name"
    rm "$zip_archive_name"
done

# Warn if no dependencies were found
if [ "$new_dependencies_found" == "false" ]; then
    echo "No Python model dependencies found"
    exit 0
fi

# Cleanup the intermediate artifacts. This isn't important on CI but
# it's helpful when developing locally
echo "Cleaning up virtualenv"
deactivate
rm -rf "$venv_name"
