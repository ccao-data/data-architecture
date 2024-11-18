import os
import re
import sys
from collections import defaultdict

import yaml


def normalize_string(s):
    """
    Normalize a string by removing non-alphanumeric characters and converting it to lowercase.

    Args:
        s (str): The input string to normalize.

    Returns:
        str: The normalized string.
    """
    return re.sub(r"[^a-zA-Z0-9]", "", s).lower()


def alphanumeric_key(s):
    """
    Generate a key for sorting strings in alphanumeric order.

    Args:
        s (str): The input string to generate the key for.

    Returns:
        list: A list of parts of the input string split into numeric and non-numeric parts.
    """
    return [
        int(text) if text.isdigit() else text.lower()
        for text in re.split("([0-9]+)", s)
    ]


def is_sorted(lst):
    """
    Check if a list of strings is sorted in alphanumeric order.

    Args:
        lst (list): A list of strings to check for sorting.

    Returns:
        bool: True if the list is sorted, False otherwise.
    """
    normalized_list = [alphanumeric_key(normalize_string(s)) for s in lst]
    return normalized_list == sorted(normalized_list)


def check_columns(file_path):
    """
    Check if the 'columns' sections in a YAML file are sorted.

    Args:
        file_path (str): The path to the YAML file to check.

    Returns:
        tuple: A dictionary with unsorted files and a list of errors encountered.
    """
    try:
        with open(file_path, "r") as file:
            # This disregards files within the check_columns() function
            # that have this tag at the top. This is limited to yaml
            # files with column sorting, if we want to expand we will
            # have to generalize it or manually add a check to the
            # other functions
            first_line = file.readline().strip()
            if first_line == "# disable-check-sort-order":
                return {}, []
            # Reset the cursor to the start of the file
            file.seek(0)
            data = yaml.safe_load(file)
    except yaml.YAMLError as error:
        return {}, [f"Error processing {file_path}: {error}"]

    def check_columns_in_yaml(
        data, file_path, unsorted_files_dict, parent_key=None
    ):
        """
        Recursively check the 'columns' sections in a YAML structure for sorting.

        Args:
            data (dict or list): The YAML data to check.
            file_path (str): The path to the YAML file being checked.
            unsorted_files_dict (dict): A dictionary to store unsorted files.
            parent_key (str, optional): The parent key of the current section being checked.
        """
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
                        print("-" * 40)
                        unsorted_files_dict[file_path] += 1
                else:
                    check_columns_in_yaml(
                        value, file_path, unsorted_files_dict, key
                    )
        elif isinstance(data, list):
            for item in data:
                check_columns_in_yaml(
                    item, file_path, unsorted_files_dict, parent_key
                )

    unsorted_files_dict = defaultdict(int)
    check_columns_in_yaml(data, file_path, unsorted_files_dict)
    return unsorted_files_dict, []


def check_data_tests(file_path):
    """
    Check if the 'data_tests' sections in a YAML file are sorted.

    Args:
        file_path (str): The path to the YAML file to check.

    Returns:
        tuple: A dictionary with unsorted files and a list of errors encountered.
    """
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
    except yaml.YAMLError as error:
        return [error], [file_path]

    def check_data_tests_in_yaml(
        data, file_path, unsorted_files_dict, parent_key=None
    ):
        """
        Recursively check the 'data_tests' sections in a YAML structure for sorting.

        Args:
            data (dict or list): The YAML data to check.
            file_path (str): The path to the YAML file being checked.
            unsorted_files_dict (dict): A dictionary to store unsorted files.
            parent_key (str, optional): The parent key of the current section being checked.
        """
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
                        print("-" * 40)
                        unsorted_files_dict[file_path] += 1
                else:
                    check_data_tests_in_yaml(
                        value, file_path, unsorted_files_dict, key
                    )
        elif isinstance(data, list):
            for item in data:
                check_data_tests_in_yaml(
                    item, file_path, unsorted_files_dict, parent_key
                )

    unsorted_files_dict = defaultdict(int)
    check_data_tests_in_yaml(data, file_path, unsorted_files_dict)
    return unsorted_files_dict, []


def check_md_file(file_path):
    """
    Check if the headings in a markdown file are sorted.

    Args:
        file_path (str): The path to the markdown file to check.

    Returns:
        str or None: The file path if unsorted headings are found, otherwise None.
    """
    with open(file_path, "r") as file:
        lines = file.readlines()

    headings = [
        line.strip()
        for line in lines
        if line.strip().startswith("#") and line.strip().count("#") == 1
    ]
    if not is_sorted(headings):
        print(f"In file: {file_path}")
        print("Headings not sorted:")
        for i, heading in enumerate(headings):
            if i > 0 and alphanumeric_key(
                normalize_string(headings[i - 1])
            ) > alphanumeric_key(normalize_string(heading)):
                print(f"---> {heading}")
            else:
                print(f"- {heading}")
        print("-" * 40)
        return file_path
    return None


def check_columns_md_file(file_path):
    """
    Check if '##' level headings in a markdown file are sorted.

    Args:
        file_path (str): The path to the markdown file to check.

    Returns:
        str or None: The file path if unsorted '##' headings are found, otherwise None.
    """
    with open(file_path, "r") as file:
        lines = file.readlines()

    headings = [
        line.strip() for line in lines if line.strip().startswith("##")
    ]
    if not is_sorted(headings):
        print(f"In file: {file_path}")
        print("'##' Headings not sorted:")
        for i, heading in enumerate(headings):
            if i > 0 and alphanumeric_key(
                normalize_string(headings[i - 1])
            ) > alphanumeric_key(normalize_string(heading)):
                print(f"---> {heading}")
            else:
                print(f"- {heading}")
        print("-" * 40)
        return file_path
    return None


def check_shared_columns_md_file(file_path):
    """
    Check if both top-level and subheadings in a markdown file are sorted.

    Args:
        file_path (str): The path to the markdown file to check.

    Returns:
        str or None: The file path if unsorted headings are found, otherwise None.
    """
    with open(file_path, "r") as file:
        lines = file.readlines()

    headings = []
    current_parent = None
    current_children = []

    for line in lines:
        stripped_line = line.strip()
        if stripped_line.startswith("# ") and stripped_line.count("#") == 1:
            if current_parent is not None:
                headings.append((current_parent, current_children))
            current_parent = stripped_line
            current_children = []
        elif stripped_line.startswith("## ") and stripped_line.count("#") == 2:
            current_children.append(stripped_line)

    if current_parent is not None:
        headings.append((current_parent, current_children))

    parent_headings = [item[0] for item in headings]
    if not is_sorted(parent_headings):
        print(f"In file: {file_path}")
        print("Top-level headings ('#') not sorted:")
        for i, heading in enumerate(parent_headings):
            if i > 0 and alphanumeric_key(
                normalize_string(parent_headings[i - 1])
            ) > alphanumeric_key(normalize_string(heading)):
                print(f"---> {heading}")
            else:
                print(f"- {heading}")
        print("-" * 40)

    any_unsorted = False
    for parent, children in headings:
        if not is_sorted(children):
            print(f"In file: {file_path}")
            print(f"Under parent heading: {parent}")
            print("Subheadings ('##') not sorted:")
            for i, child in enumerate(children):
                if i > 0 and alphanumeric_key(
                    normalize_string(children[i - 1])
                ) > alphanumeric_key(normalize_string(child)):
                    print(f"---> {child}")
                else:
                    print(f"- {child}")
            print("-" * 40)
            any_unsorted = True

    return (
        file_path if not is_sorted(parent_headings) or any_unsorted else None
    )


def check_all_files(directory):
    """
    Check all files in a given directory for sorted YAML keys and markdown headings.

    Args:
        directory (str): The directory path to check.

    Returns:
        tuple: Results of unsorted files and errors for different checks.
    """
    file_paths_to_check = []
    for root, _, files in os.walk(directory):
        if "venv" in root:
            continue
        for file in files:
            if (
                file.endswith(".yaml")
                or file.endswith(".yml")
                or (file in ("docs.md", "columns.md", "shared_columns.md"))
            ):
                file_paths_to_check.append(os.path.join(root, file))

    return check_files(file_paths_to_check)


def check_files(file_paths: list[str]):
    """
    Check all files in a list of filepaths for sorted YAML keys and markdown
    headings.

    Args:
        file_paths (list[str]): The list of files to check

    Returns:
        tuple: Results of unsorted files and errors for different checks.
    """
    unsorted_columns_files: dict[str, int] = defaultdict(int)
    unsorted_data_tests_files: dict[str, int] = defaultdict(int)
    error_files = []
    unsorted_md_files = []
    unsorted_columns_md_files = []
    unsorted_shared_columns_md_files = []
    for file_path in file_paths:
        if not os.path.isfile(file_path):
            raise ValueError(
                f"check_files got a filepath that doesn't exist: {file_path}"
            )
        if file_path.endswith(".yaml") or file_path.endswith(".yml"):
            unsorted_columns, errors = check_columns(file_path)
            for key, value in unsorted_columns.items():
                unsorted_columns_files[key] += value
            unsorted_data_tests, errors = check_data_tests(file_path)
            for key, value in unsorted_data_tests.items():
                unsorted_data_tests_files[key] += value
            if errors:
                error_files.extend(errors)
        elif os.path.basename(file_path) == "docs.md":
            unsorted_md = check_md_file(file_path)
            if unsorted_md:
                unsorted_md_files.append(unsorted_md)
        elif os.path.basename(file_path) == "columns.md":
            unsorted_columns_md = check_columns_md_file(file_path)
            if unsorted_columns_md:
                unsorted_columns_md_files.append(unsorted_columns_md)
        elif os.path.basename(file_path) == "shared_columns.md":
            unsorted_shared_columns_md = check_shared_columns_md_file(
                file_path
            )
            if unsorted_shared_columns_md:
                unsorted_shared_columns_md_files.append(
                    unsorted_shared_columns_md
                )

    return (
        unsorted_columns_files,
        unsorted_data_tests_files,
        error_files,
        unsorted_md_files,
        unsorted_columns_md_files,
        unsorted_shared_columns_md_files,
    )


if __name__ == "__main__":
    args = sys.argv[1:]
    if args:
        (
            unsorted_columns_files,
            unsorted_data_tests_files,
            error_files,
            unsorted_md_files,
            unsorted_columns_md_files,
            unsorted_shared_columns_md_files,
        ) = check_files(args)
    else:
        (
            unsorted_columns_files,
            unsorted_data_tests_files,
            error_files,
            unsorted_md_files,
            unsorted_columns_md_files,
            unsorted_shared_columns_md_files,
        ) = check_all_files(os.getcwd())

    if unsorted_columns_files:
        print("The following files have unsorted columns:")
        for file, count in unsorted_columns_files.items():
            print(f"{file} ({count})")

    if unsorted_data_tests_files:
        print("\nThe following files have unsorted data tests:")
        for file, count in unsorted_data_tests_files.items():
            print(f"{file} ({count})")

    if unsorted_md_files:
        print("\nThe following markdown files have unsorted headings:")
        for file in unsorted_md_files:
            print(f"{file} (1)")

    if unsorted_columns_md_files:
        print("\nThe following columns.md files have unsorted '##' headings:")
        for file in unsorted_columns_md_files:
            print(f"{file} (1)")

    if unsorted_shared_columns_md_files:
        print(
            "\nThe following shared_columns.md files have unsorted headings:"
        )
        for file in unsorted_shared_columns_md_files:
            print(f"{file} (1)")

    print("\n")
    if (
        unsorted_columns_files
        or unsorted_data_tests_files
        or error_files
        or unsorted_md_files
        or unsorted_columns_md_files
        or unsorted_shared_columns_md_files
    ):
        raise ValueError(
            "Column name, data test, or heading sort order check ran into failures, see logs above"
        )

    print(
        "All files have sorted columns, data tests, headings, and no errors."
    )
