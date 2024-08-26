import os
import yaml
import re


def normalize_string(s):
    """Remove special characters for comparison."""
    return re.sub(r"[^a-zA-Z0-9]", "", s).lower()


def alphanumeric_key(s):
    """Turn a string into a list of strings and numbers to handle sorting."""
    return [
        int(text) if text.isdigit() else text.lower()
        for text in re.split("([0-9]+)", s)
    ]


def is_sorted(lst):
    """Check if a list is sorted, using alphanumeric sorting."""
    normalized_list = [alphanumeric_key(normalize_string(s)) for s in lst]
    return normalized_list == sorted(normalized_list)


def check_yaml_file(file_path):
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
    except yaml.YAMLError as error:
        # print(f"Error loading YAML file {file_path}: {e}")
        return [error], [file_path]  # Return as an error

    def check_columns(data, file_path, unsorted_files, parent_key=None):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "columns":
                    column_names = [
                        col.get("name")
                        for col in value
                        if isinstance(col, dict) and "name" in col
                    ]
                    if not is_sorted(column_names):
                        print(f"In file: {file_path}")
                        print(f"Key above 'columns': {parent_key}")
                        print("Columns in this group:")
                        for i, name in enumerate(column_names):
                            normalized_name = alphanumeric_key(
                                normalize_string(name)
                            )
                            if (
                                i > 0
                                and alphanumeric_key(
                                    normalize_string(column_names[i - 1])
                                )
                                > normalized_name
                            ):
                                print(f"---> {name}")
                            else:
                                print(f"- {name}")
                        print("-" * 40)  # Separator for clarity
                        unsorted_files.append(file_path)
                else:
                    check_columns(value, file_path, unsorted_files, key)
        elif isinstance(data, list):
            for item in data:
                check_columns(item, file_path, unsorted_files, parent_key)

    unsorted_files = []
    check_columns(data, file_path, unsorted_files)
    return unsorted_files, []


def check_all_yaml_files(directory):
    unsorted_files = []
    error_files = []
    for root, _, files in os.walk(directory):
        if "venv" in root:  # Skip virtual environment directories
            continue
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                file_path = os.path.join(root, file)
                unsorted, errors = check_yaml_file(file_path)
                if unsorted:
                    unsorted_files.extend(unsorted)
                if errors:
                    error_files.extend(errors)

    return unsorted_files, error_files


if __name__ == "__main__":
    directory = "dbt/"  # Change this to your dbt directory if different
    unsorted_files, error_files = check_all_yaml_files(directory)

    if unsorted_files:
        print("The following files have unsorted columns:")
        for file in unsorted_files:
            print(file)

    if error_files:
        print("\nThe following files could not be processed due to errors:")
        for file in error_files:
            print(file)

    if unsorted_files or error_files:
        exit(1)  # Exit with a status code of 1 to indicate failure
    else:
        print("All files have sorted columns and no errors.")
        exit(0)  # Exit with a status code of 0 to indicate success
