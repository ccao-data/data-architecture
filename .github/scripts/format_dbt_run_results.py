#!/usr/bin/env python3
#
# Parse a dbt run_results.json file and reformat it so that it can be uploaded
# to AWS Glue and analyzed in Athena.
#
# Example usage:
#
#   python3 format_dbt_run_results.py > target/run_results.parquet
#
import datetime
import os
import sys
import typing

import pyarrow
import pyarrow.parquet
import simplejson as json

DEFAULT_RUN_RESULTS_FILEPATH = os.path.join("target", "run_results.json")
VALID_OUTPUT_FORMATS = ["parquet", "json"]
DEFAULT_OUTPUT_FORMAT = "parquet"


def main() -> None:
    """Main entrypoint for this script. Reads in params from stdin and writes
    a formatted file to stdout."""
    # Parse and validate input arguments
    run_results_filepath, format = parse_input_arguments(sys.argv)
    with open(run_results_filepath) as run_results_fobj:
        run_results_obj = json.load(run_results_fobj, use_decimal=True)

    # Format test-level metadata from the results object
    formatted_run_results = format_run_results(run_results_obj["results"])

    # Write the output to stdout
    if format == "json":
        json.dump(formatted_run_results, sys.stdout, default=str)
    else:
        table = pyarrow.Table.from_pylist(formatted_run_results)
        pyarrow.parquet.write_table(table, sys.stdout.buffer)


def parse_input_arguments(
    sys_argv: typing.List[str],
) -> typing.Tuple[str, str]:
    """Helper function to parse arguments from stdin and return them for use
    by a calling script. Note that this would be more efficient with argparse
    or click, but for now the interface is simple enough that we implement
    parsing manually."""
    valid_output_format_str = ""
    for idx, format in enumerate(VALID_OUTPUT_FORMATS):
        if idx > 0:
            separator = (
                " or " if idx == len(VALID_OUTPUT_FORMATS) - 1 else ", "
            )
            valid_output_format_str += separator
        valid_output_format_str += f"'{format}'"

    argument_error = (
        "Invalid arguments: Must include one positional argument representing "
        "the path to a run_results.json file, and/or a --format option with "
        f"the value {valid_output_format_str}"
    )
    if len(sys_argv) == 1:
        run_results_filepath = DEFAULT_RUN_RESULTS_FILEPATH
        format = DEFAULT_OUTPUT_FORMAT

    elif len(sys_argv) == 2:
        if sys_argv[1] == "--format":
            raise ValueError(argument_error)
        run_results_filepath = sys_argv[1]
        format = DEFAULT_OUTPUT_FORMAT

    elif len(sys_argv) == 3:
        if sys_argv[1] == "--format":
            format = sys_argv[2]
            run_results_filepath = DEFAULT_RUN_RESULTS_FILEPATH
        else:
            raise ValueError(argument_error)

    elif len(sys_argv) == 4:
        if sys_argv[1] == "--format":
            format = sys_argv[2]
            run_results_filepath = sys_argv[3]
        elif sys_argv[2] == "--format":
            format = sys_argv[3]
            run_results_filepath = sys_argv[2]
        else:
            raise ValueError(argument_error)

    else:
        raise ValueError(argument_error)

    assert (
        format in VALID_OUTPUT_FORMATS
    ), f"--format must be one of {valid_output_format_str}"

    return run_results_filepath, format


def format_run_results(
    run_results: typing.List[typing.Dict],
) -> typing.List[typing.Dict]:
    """Given a list of dicts representing failed test metadata extracted from
    a dbt run_results.json file, return a list of dicts that reformats that
    metadata into a format that we expect."""
    formatted_run_results = []
    for run_result in run_results:
        formatted_result = {
            "status": run_result["status"],
            "execution_time": run_result["execution_time"],
            "num_failing_rows": run_result["failures"],
            # Parse the name of the test from the unique_id. The unique_id is
            # always formatted like:
            # `test.athena.<test_name>.<hash>`
            "test_name": run_result["unique_id"].split(".")[2],
        }

        # Unpivot the list of objects representing timings for different
        # stages of test execution to transform it from a list of objects into
        # flat attributes. An example of the raw list would look like this:
        #
        #    "timing": [
        #      {
        #        "name": "compile",
        #        "started_at": "2024-03-26T17:54:28.865113Z",
        #        "completed_at": "2024-03-26T17:54:28.872620Z"
        #      },
        #      {
        #        "name": "execute",
        #        "started_at": "2024-03-26T17:54:28.873222Z",
        #        "completed_at": "2024-03-26T17:54:35.137882Z"
        #      }
        #    ]
        #
        # In this case, we unpivot the list into four separate attributes:
        #
        #    "timing_compile_started_at": "2024-03-26T17:54:28.865113Z",
        #    "timing_compile_completed_at": "2024-03-26T17:54:28.872620Z",
        #    "timing_execute_started_at": "2024-03-26T17:54:28.873222Z",
        #    "timing_execute_completed_at": "2024-03-26T17:54:35.137882Z"
        for timing_dict in run_result["timing"]:
            key_prefix = f"timing_{timing_dict['name']}"
            for timing_val in ["started_at", "completed_at"]:
                timing_dt = datetime.datetime.strptime(
                    timing_dict[timing_val],
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                )
                formatted_result[f"{key_prefix}_{timing_val}"] = timing_dt

        formatted_run_results.append(formatted_result)

    return formatted_run_results


if __name__ == "__main__":
    main()
