#!/usr/bin/env bash
# Parse, bundle, and upload dbt Python model requirements to S3.
#
# Takes one or more optional positional arguments representing the models for
# which you would like to upload dependencies. For example, the following
# command will only upload dependencies for the `reporting.ratio_stats` model:
#
# ../.github/scripts/deploy_dbt_model_dependencies.sh reporting.ratio_stats
#
# When no arguments are provided, the script will upload dependencies for all
# models:
#
# ../.github/scripts/deploy_dbt_model_dependencies.sh
#
# Assumes that dbt, python, and jq are installed and available on the caller's
# path. Also assumes that it is being run in a directory containing a
# dbt project.
set -euo pipefail

# Fall back to local development if a dbt $TARGET is not specified
target=${TARGET:-"dev"}
echo "Parsing dbt Python model dependencies for target '$target'"

# Determine the remote location where bundled dependencies will be deployed
s3_dependency_dir="s3://ccao-dbt-dependencies-us-east-1"

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

# Set a flag to check whether any dependencies were found so we can log
# a warning if none are found
dependencies_found=false

# Iterate over each key-value pair representing a set of package
# dependencies and output those dependencies to a requirements file
# TODO: Gotta figure out this loop next
for package in "${packages_array[@]}"; do
    # Set the flag to confirm dependencies were found, since if we reach this
    # point it means that packages_array is non-empty
    dependencies_found=true

    # Write the list of dependencies to the requirements file
    # echo "$dependencies" | tr ' ' '\n' >> "$requirements_filename"
    echo "$package"
done

# Warn if no dependencies were found
if [ "$dependencies_found" == "false" ]; then
    echo "No Python model dependencies found"
    exit 0
fi


# Check if the archive already exists on S3
existing_requirements_file_url="${s3_dependency_dir}/${requirements_filename}"
if aws s3 ls "$existing_requirements_file_url" > /dev/null 2>&1; then
    echo "Diffing against $existing_requirements_file_url to check for changes"

    # Create a temporary directory to download the S3 file
    temp_dir=$(mktemp -d)
    existing_requirements_filename="${temp_dir}/${requirements_filename}"

    # Download the S3 file to the temporary directory
    aws s3 cp "$existing_requirements_file_url" "$existing_requirements_filename" --no-progress

    # Sort and compare the contents of the requirements files
    skip_upload=false
    if diff <(sort "$requirements_filename") <(sort "$existing_requirements_filename") > /dev/null; then
        echo "Skipping upload for $requirements_filename since it has not changed"
        skip_upload=true
    else
        echo "Proceeding with upload for $requirements_filename since it has changed"
    fi

    # Clean up the temporary directory, since it's no longer needed
    rm -rf "$temp_dir"

    if [ "$skip_upload" == "true" ]; then
        # Normally we clean up the requirements file after upload, but if
        # we're not going to upload the file, we need to clean it up now
        rm "$requirements_filename"
        exit 0
    fi
fi

# Create and activate a Python virtual environment for installing dependencies
venv_name="${model_identifier}.venv"
echo "Creating and activating virtualenv at $venv_name"
python3 -m venv "$venv_name"
# Stop shellcheck from getting mad that the virtualenv dir doesn't exist at
# compile time. It's not a problem since the Python `venv` command will
# take care of creating it
# shellcheck disable=SC1091
source "${venv_name}/bin/activate"

# Install dependencies into a subdirectory that we can use for bundling
subdirectory_name="${model_identifier}/"
mkdir -p "$subdirectory_name"
echo "Installing dependencies from $requirements_filename into $subdirectory_name"
pip install -t "$subdirectory_name" -r "$requirements_filename" --no-deps

# Create a zip archive from the contents of the subdirectory
zip_archive_name="${model_identifier}.requirements.zip"
echo "Creating zip archive $zip_archive_name from $subdirectory_name"
cd "$subdirectory_name" && zip -q -r9 "../$zip_archive_name" ./* && cd ..

# Upload the zip archive and the requirements file to S3
echo "Uploading $zip_archive_name and $requirements_filename to S3"
aws s3 cp "$zip_archive_name" "${s3_dependency_dir}/" --no-progress
aws s3 cp "$requirements_filename" "${s3_dependency_dir}/" --no-progress

# Cleanup the intermediate artifacts. This isn't important on CI but
# it's helpful when developing locally
echo "Cleaning up intermediate artifacts"
deactivate
rm "$requirements_filename"
rm -rf "$venv_name"
rm -rf "$subdirectory_name"
rm "$zip_archive_name"
