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
from utils.townships import (
    TOWNSHIP_SCHEDULE_PATH,
    TOWNSHIPS,
    TOWNSHIPS_BY_CODE,
)

DBT = dbtRunner()
CURRENT_YEAR = datetime.datetime.now().year
CURRENT_DATE = datetime.datetime.now().date()

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
        required=False,
        nargs="*",
        help=(
            "One or more space-separated township codes to use when filtering "
            "query results. When missing, defaults to all towns that are "
            "eligible for QC, i.e. towns that are currently between the field "
            "check and completion stage of the 1st or 2nd pass valuation "
            "cycles. This default will only work if a data file exists at "
            "scripts/utils/town_active_schedule.csv, and will raise an error "
            "otherwise. This behavior is intended for use in the automated "
            "version of this script that runs on a schedule, so human callers "
            "should generally not need it"
        ),
    )
    parser.add_argument(
        "--year",
        required=False,
        default=CURRENT_YEAR,
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
    parser.add_argument(
        *constants.OUTPUT_DIR_ARGUMENT_ARGS,
        **constants.OUTPUT_DIR_ARGUMENT_KWARGS,
    )
    return parser.parse_args()


def main():
    """Main entrypoint for the script"""
    args = parse_args()

    # Determine the default set of towns based on 'active' status
    if args.township:
        townships = [TOWNSHIPS_BY_CODE[code] for code in args.township]
    else:
        if args.year != CURRENT_YEAR:
            # We can't determine active towns for prior years, so raise an
            # error if the caller tries to do this
            raise ValueError(
                "--township must be set if --year is not the current year"
            )
        townships = [
            town for town in TOWNSHIPS if town.is_active_on(CURRENT_DATE)
        ]
        if not townships:
            raise ValueError(
                "No townships are active in the current year. Double check "
                f"{TOWNSHIP_SCHEDULE_PATH.resolve()} to ensure the schedule "
                "is correct"
            )

    # Determine which dbt tags to select based on the tri status of each town
    base_tag = "tag:qc_report_town_close"
    tags = {base_tag}
    for town in townships:
        tag_suffix = (
            "tri" if town.is_reassessed_during(args.year) else "non_tri"
        )
        tags.add(f"{base_tag}_{tag_suffix}")
    select = list(tags)

    if args.print_table_refresh_command:
        # Use `dbt list` on parents to calculate the update command for
        # iasworld sources that are dependencies for the models we want to
        # export
        dbt_list_args = [
            "--quiet",
            "list",
            "--target",
            args.target,
            # Filter results for sources so that the `dbt list` call only
            # returns iasWorld assets. We'll have to tweak this if we
            # ever introduce town close reports that depend on non-iasWorld
            # sources, like our spatial tables
            "--resource-types",
            "source",
            "--output",
            "json",
            "--output-keys",
            "name",
            "source_name",
            "--select",
            # Prepending a plus sign to each element of the select list
            # instructs dbt to select their parents as well
            *[f"+{tag}" for tag in select],
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
        if len(townships) > 1:
            # Format a list of township codes for templating into a SQL array,
            # which requires wrapping each code in single quotes
            township_code_sql_str = "', '".join(
                town.township_code for town in townships
            )
            where = (
                f"township_code IN ('{township_code_sql_str}') "
                f"AND taxyr = '{args.year}'"
            )
            export_models(
                target=args.target,
                rebuild=args.rebuild,
                select=select,
                where=where,
                order_by=["township_code"],
                output_dir=args.output_dir,
            )
        else:
            township = townships[0]
            where = (
                f"township_code = '{township.township_code}' "
                f"AND taxyr = '{args.year}'"
            )
            output_paths = export_models(
                target=args.target,
                rebuild=args.rebuild,
                select=select,
                where=where,
                output_dir=args.output_dir,
            )
            # Rename the output files to prepend the name of the township
            for output_path in output_paths:
                new_filename = f"{township.township_name} {output_path.name}"
                new_filepath = output_path.with_name(new_filename)
                output_path.rename(new_filepath)
                print(f"Renamed {output_path} -> {new_filepath}")


if __name__ == "__main__":
    main()
