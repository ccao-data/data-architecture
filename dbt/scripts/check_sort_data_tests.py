import os
import yaml
import re
from collections import defaultdict


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


def check_data_tests(file_path):
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
    except yaml.YAMLError as error:
        return [error], [file_path]  # Return as an error

    def check_data_tests_in_columns(
        data, file_path, unsorted_files_dict, parent_key=None
    ):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "data_tests" and isinstance(value, list):
                    data_test_names = []
                    for test in value:
                        if isinstance(test, dict):
                            for test_type, test_details in test.items():
                                if (
                                    isinstance(test_details, dict)
                                    and "name" in test_details
                                ):
                                    data_test_names.append(
                                        (test_type, test_details["name"], test)
                                    )

                    # Sort and compare
                    sorted_tests = sorted(
                        data_test_names,
                        key=lambda x: alphanumeric_key(normalize_string(x[1])),
                    )
                    if data_test_names != sorted_tests:
                        print(f"In file: {file_path}")
                        print(f"Key above 'data_tests': {parent_key}")
                        print("Data tests in this group are not sorted:")
                        for i, (_, name, _) in enumerate(data_test_names):
                            if name != sorted_tests[i][1]:
                                print(f"---> {name}")
                            else:
                                print(f"- {name}")
                        print("-" * 40)  # Separator for clarity
                        unsorted_files_dict[file_path] += 1  # Increment count
                else:
                    check_data_tests_in_columns(
                        value, file_path, unsorted_files_dict, key
                    )
        elif isinstance(data, list):
            for item in data:
                check_data_tests_in_columns(
                    item, file_path, unsorted_files_dict, parent_key
                )

    unsorted_files_dict = defaultdict(int)
    check_data_tests_in_columns(data, file_path, unsorted_files_dict)
    return unsorted_files_dict, []


def check_all_yaml_files_for_data_tests(directory):
    unsorted_files_dict = defaultdict(int)
    error_files = []
    for root, _, files in os.walk(directory):
        if "venv" in root:  # Skip virtual environment directories
            continue
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                file_path = os.path.join(root, file)
                unsorted, errors = check_data_tests(file_path)
                for key, value in unsorted.items():
                    unsorted_files_dict[key] += value
                if errors:
                    error_files.extend(errors)

    return unsorted_files_dict, error_files


if __name__ == "__main__":
    directory = "dbt/"
    unsorted_files_dict, error_files = check_all_yaml_files_for_data_tests(
        directory
    )

    if unsorted_files_dict:
        print("The following files have unsorted data tests:")
        for file, count in unsorted_files_dict.items():
            if count > 1:
                print(f"{file} ({count})")
            else:
                print(file)

    if error_files:
        print("\nThe following files could not be processed due to errors:")
        for file in error_files:
            print(file)

    if unsorted_files_dict or error_files:
        exit(1)  # Exit with a status code of 1 to indicate failure
    else:
        print("All files have sorted data tests and no errors.")
        exit(0)  # Exit with a status code of 0 to indicate success
