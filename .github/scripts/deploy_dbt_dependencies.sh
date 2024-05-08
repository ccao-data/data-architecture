#!/usr/bin/env bash
# Parse, bundle, and upload dbt Python model requirements to S3.
#
# Assumes that dbt, python, and jq are installed and available on the caller's
# path.
set -euo pipefail

# Fall back to local development if a dbt $TARGET is not specified
target=${TARGET:-"dev"}

# Set the remote location where bundled dependencies will be deployed
base_s3_url="s3://ccao-dbt-athena-ci-us-east-1/packages/"

# Compile the DAG so that we have up-to-date info on dependencies
echo "Parsing dbt Python model dependencies for target '$target'"
dbt compile -q -t "$target"

# Extract config.packages attributes from models
packages_json=$(jq '
    .nodes
    | with_entries(
        select(
            .value.config.packages != null and
            (.value.config.packages | length) > 0
        )
    )
    | with_entries(
        .value = .value.config.packages
    )
' target/manifest.json)

# Set a flag to check whether any dependencies were found
dependencies_found=false

# Iterate over each key-value pair representing a set of package
# dependencies and output those dependencies to a requirements file.
# Note that the input to the `read` call is passed in using process
# substitution so that we can avoid a subshell and thereby modify the
# global $dependencies_found variable in the context of the loop
while read -r item; do
    # Set the flag to confirm dependencies were found
    dependencies_found=true

    # Extract the key and value
    model_name=$(echo "$item" | jq -r '.model_name')
    dependencies=$(echo "$item" | jq -r '.dependencies[]')

    # Split the key by '.' and take the last two elements
    model_identifier=$(echo "$model_name" | awk -F. '{print $(NF-1)"."$NF}')

    # Define the filename for the requirements file
    requirements_filename="${model_identifier}.requirements.txt"

    # Create the file and write the contents
    echo "$dependencies" | tr ' ' '\n' > "$requirements_filename"
    echo "Python requirements file $requirements_filename created with contents:"
    cat "$requirements_filename"

    # Check if the archive already exists on S3
    existing_requirements_file_url=${base_s3_url}${requirements_filename}
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
            continue
        fi
    fi

    # Create and activate a Python virtual environment for installing dependencies
    venv_name="${model_identifier}.venv"
    echo "Creating and activating virtualenv at $venv_name"
    python3 -m venv "$venv_name"
    source "${venv_name}/bin/activate"

    # Install dependencies into a subdirectory
    subdirectory_name="${model_identifier}/"
    mkdir -p "$subdirectory_name"
    echo "Installing dependencies from $requirements_filename into $subdirectory_name"
    pip install -t "$subdirectory_name" -r "$requirements_filename"

    # Create a zip archive from the contents of the subdirectory
    zip_archive_name="${model_identifier}.requirements.zip"
    echo "Creating zip archive $zip_archive_name from $subdirectory_name"
    zip -q -r "$zip_archive_name" "$subdirectory_name"

    # Upload the archive to S3
    echo "Uploading $zip_archive_name and $requirements_filename to S3"
    aws s3 cp "$zip_archive_name" "$base_s3_url" --no-progress
    aws s3 cp "$requirements_filename" "$base_s3_url" --no-progress

    # Cleanup the intermediate artifacts
    echo "Cleaning up intermediate artifacts"
    deactivate
    rm "$requirements_filename"
    rm -rf "$venv_name"
    rm -rf "$subdirectory_name"
    rm "$zip_archive_name"

done < <(
    echo "$packages_json" | \
        jq -rc 'to_entries[] | {model_name: .key, dependencies: .value}'
)

# Warn if no dependencies were found
if [ "$dependencies_found" == "false" ]; then
    echo "No Python model dependencies found"
fi
