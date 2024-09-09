# Export town close QC reports to Excel files.
#
# Run `python scripts/export_qc_town_close_reports.py --help` for details.
import argparse
import contextlib
import datetime
import io
import json

from dbt.cli.main import dbtRunner
from utils import constants
from utils.export import export_models

DBT = dbtRunner()

CLI_DESCRIPTION = """Export town close QC reports to Excel files.

Expects dependencies from requirements.txt (dbt dependencies) and scripts/requirements.export_models.txt (script dependencies) be installed.

The queries that generate these reports run against our data warehouse, which ingests data from iasWorld overnight once daily. Sometimes a
staff member will request a report during the middle of the workday, and they will need the most recent data, which will not exist in
our warehouse yet. In these cases, you can use the --print-table-refresh-command flag to output a command that you can run on the server to refresh
any iasWorld tables that these reports use.
"""  # noqa: E501
CLI_EXAMPLE = """Example usage to output the 2024 town close QC report for Hyde Park:

    python scripts/export_qc_town_close_reports.py --township 70 --year 2024

To output the town close QC report for Hyde Park in the current year:

    python scripts/export_qc_town_close_reports.py --township 70

To get a command to run to refresh iasWorld tables prior to export:

    python scripts/export_qc_town_close_reports.py --township 70 --year 2024 --print-table-refresh-command
"""  # noqa: E501

# Map township codes to their triad number
TOWNSHIP_TO_TRI = {
    "10": "2",
    "11": "3",
    "12": "3",
    "13": "3",
    "14": "3",
    "15": "3",
    "16": "2",
    "17": "2",
    "18": "2",
    "19": "3",
    "20": "2",
    "21": "3",
    "22": "2",
    "23": "2",
    "24": "2",
    "25": "2",
    "26": "2",
    "27": "3",
    "28": "3",
    "29": "2",
    "30": "3",
    "31": "3",
    "32": "3",
    "33": "3",
    "34": "3",
    "35": "2",
    "36": "3",
    "37": "3",
    "38": "2",
    "39": "3",
    "70": "1",
    "71": "1",
    "72": "1",
    "73": "1",
    "74": "1",
    "75": "1",
    "76": "1",
    "77": "1",
}


def is_tri(township_code: str, year: int) -> bool:
    """Helper function to determine if a town in a given year is undergoing
    triennial reassessment"""
    try:
        tri = TOWNSHIP_TO_TRI[township_code]
    except KeyError:
        raise ValueError(f"'{township_code}' is not a valid township code")
    # 2024 is the City reassessment year (tri code 1), so
    # ((2024 - 2024) % 3) + 1 == 1, and so on for the other two tris
    return str((year - 2024 % 3) + 1) == tri


def parse_args() -> argparse.Namespace:
    """Helper function to parse arguments to this script"""
    parser = argparse.ArgumentParser(
        description=CLI_DESCRIPTION,
        epilog=CLI_EXAMPLE,
        # Parse the description and epilog as raw text so that newlines
        # get preserved
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        *constants.TARGET_ARGUMENT_ARGS, **constants.TARGET_ARGUMENT_KWARGS
    )
    parser.add_argument(
        *constants.REBUILD_ARGUMENT_ARGS, **constants.REBUILD_ARGUMENT_KWARGS
    )
    parser.add_argument(
        "--township",
        required=True,
        help="Township code to use in filtering query results",
    )
    parser.add_argument(
        "--year",
        required=False,
        default=datetime.datetime.now().year,
        type=int,
        help="Tax year to use in filtering query results. Defaults to the current year",
    )
    parser.add_argument(
        "--print-table-refresh-command",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Print a command that can be run on the server to refresh "
            "underlying iasWorld tables and exit. Useful if you want to "
            "refresh table data before running exports"
        ),
    )
    return parser.parse_args()


def main():
    """Main entrypoint for the script"""
    args = parse_args()
    tag = "tag:qc_report_town_close"
    tag_suffix = "tri" if is_tri(args.township, args.year) else "non_tri"

    if args.print_table_refresh_command:
        # Use `dbt list` on parents to calculate update command for
        # iasworld sources that are implicated by this call
        dbt_list_args = [
            "--quiet",
            "list",
            "--target",
            args.target,
            "--resource-types",
            "source",
            "--output",
            "json",
            "--output-keys",
            "name",
            "source_name",
            "--select",
            f"+{tag}",
            f"+{tag}_{tag_suffix}",
        ]
        dbt_output = io.StringIO()
        with contextlib.redirect_stdout(dbt_output):
            dbt_list_result = DBT.invoke(dbt_list_args)

        if not dbt_list_result.success:
            print("Encountered error in `dbt list` call")
            raise ValueError(dbt_list_result.exception)

        # Output is formatted as a list of newline-separated JSON objects
        source_deps = [
            json.loads(source_dict_str)
            for source_dict_str in dbt_output.getvalue().split("\n")
            # Filter out empty strings caused by trailing newlines
            if source_dict_str
        ]
        # Generate a Spark job definition for each iasWorld table that needs
        # to be updated. For more context on what these attributes mean, see
        # https://github.com/ccao-data/service-spark-iasworld
        iasworld_deps = {}
        for dep in source_deps:
            table_name = dep["name"]
            formatted_dep = {
                "table_name": f"iasworld.{table_name}",
                "min_year": args.year,  # Restrict to current year of data
                "cur": ["Y"],  # Only get active records
            }
            # We should figure out a way to programmatically determine the
            # correct filters for each table, but in the meantime, hardcode
            # them based on known exceptions
            if table_name == "sales":
                # The sales table doesn't have a taxyr column
                formatted_dep["min_year"] = formatted_dep["max_year"] = None
            if table_name == "asmt_hist":
                # The asmt_hist table doesn't have any cur = 'Y' records
                del formatted_dep["cur"]

            iasworld_deps[table_name] = formatted_dep

        print("ssh into the server and run the following commands:")
        print()
        print("cd /home/shiny-server/services/service-spark-iasworld")
        print("docker-compose up -d")
        print(
            "docker exec spark-node-master ./submit.sh "
            "--no-run-github-workflow "
            f"--json-string '{json.dumps(iasworld_deps)}' "
        )
    else:
        where = f"township_code = '{args.township}' and taxyr = '{args.year}'"
        export_models(
            target=args.target,
            rebuild=args.rebuild,
            select=[tag, f"{tag}_{tag_suffix}"],
            where=where,
        )


if __name__ == "__main__":
    main()
