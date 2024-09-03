import os
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


def check_headings_in_docs(file_path):
    """Check if headings in docs.md files are sorted."""
    try:
        with open(file_path, "r") as file:
            lines = file.readlines()
    except IOError as error:
        return [error], [file_path]  # Return as an error

    # Extract headings starting with '#'
    headings = [line.strip() for line in lines if line.startswith("#")]
    if not is_sorted(headings):
        print(f"Headings in {file_path} are not sorted:")
        for heading in headings:
            print(f"- {heading}")
        print("-" * 40)  # Separator for clarity
        return {file_path: 1}, []  # Return the file as unsorted

    return {}, []  # Return an empty dictionary if sorted


def check_all_docs_files(directory):
    unsorted_files_dict = defaultdict(int)
    error_files = []
    for root, _, files in os.walk(directory):
        if "venv" in root:  # Skip virtual environment directories
            continue
        for file in files:
            if file == "docs.md":
                file_path = os.path.join(root, file)
                unsorted, errors = check_headings_in_docs(file_path)
                for key, value in unsorted.items():
                    unsorted_files_dict[key] += value
                if errors:
                    error_files.extend(errors)

    return unsorted_files_dict, error_files


if __name__ == "__main__":
    directory = "dbt/"
    unsorted_files_dict, error_files = check_all_docs_files(directory)

    if unsorted_files_dict:
        print("The following docs.md files have unsorted headings:")
        for file, count in unsorted_files_dict.items():
            if count > 1:
                print(f"{file} ({count})")
            else:
                print(file)

    if error_files:
        print(
            "\nThe following docs.md files could not be processed due to errors:"
        )
        for file in error_files:
            print(file)

    if unsorted_files_dict or error_files:
        raise ValueError(
            "Heading sort order check ran into failures, see logs above"
        )
    print("All docs.md files have sorted headings and no errors.")
