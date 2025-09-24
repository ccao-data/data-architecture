# Export town close QC reports to Excel files.
#
# Run `python scripts/export_qc_town_close_reports.py --help` for details.
import argparse
import contextlib
import dataclasses
import datetime
import io
import json
import pathlib

from dbt.cli.main import dbtRunner
from utils import constants
from utils.export import (
    ModelForExport,
    query_models_for_export,
    save_model_to_workbook,
)
from utils.townships import (
    TOWNSHIPS,
    TOWNSHIPS_BY_CODE,
    TOWNSHIPS_BY_NAME,
    Township,
)

DBT = dbtRunner()

CLI_DESCRIPTION = """Export town close QC reports to Excel files.

Expects dependencies from [project].dependencies (dbt dependencies) and [project.optional-dependencies].dbt_tests (script dependencies) be installed.

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
            "query results. When missing, defaults to all towns."
        ),
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
    parser.add_argument(
        *constants.OUTPUT_DIR_ARGUMENT_ARGS,
        **constants.OUTPUT_DIR_ARGUMENT_KWARGS,
    )
    return parser.parse_args()


def main():
    """Main entrypoint for the script"""
    args = parse_args()

    townships: list[Township] = []
    for code in args.township:
        if town := TOWNSHIPS_BY_CODE.get(code):
            townships.append(town)
        elif town := TOWNSHIPS_BY_NAME.get(code.lower()):
            townships.append(town)
        else:
            raise ValueError(f"Town code/name not recognized: {code}")
    else:
        townships = TOWNSHIPS

    # Determine which dbt tags to select based on the tri status of each town
    # (i.e. whether the town is up for reassessment in the given year).
    # Models that apply to both tri and non-tri towns use the tag
    # `qc_report_town_close`, while tri and non-tri towns add a suffix to this
    # base tag (`_tri` and `_non_tri`, respectively)
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
        # iasWorld sources that are dependencies for the models we want to
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

        print(
            "Run the following commands on the Data Team",
            "server as the shiny-server user:",
        )
        print()
        print("cd /home/shiny-server/services/service-spark-iasworld")
        print("docker compose --profile prod up -d")
        print(
            "docker exec spark-node-master-prod ./submit.sh",
            "--upload-data --upload-logs --run-glue-crawler",
            f"--json-string '{json.dumps(iasworld_deps)}'",
        )
    else:
        models_for_export = query_models_for_export(
            target=args.target,
            rebuild=args.rebuild,
            select=select,
            where=f"taxyr = '{args.year}'",
        )
        output_dir = args.output_dir or "export/output/"
        for model in models_for_export:
            for town in townships:
                # Only produce an export for this town if the report model
                # is configured for its reassessment year
                town_is_tri = town.is_reassessed_during(args.year)
                if f"{base_tag}_tri" in model.tags and not town_is_tri:
                    print(
                        f"Skipping {model.name} for {town.township_name} "
                        "because the report is only for tri towns"
                    )
                    continue
                elif f"{base_tag}_non_tri" in model.tags and town_is_tri:
                    print(
                        f"Skipping {model.name} for {town.township_name} "
                        "because the report is only for non-tri towns"
                    )
                    continue

                # Filter the query results for only this town, but only if the
                # query results are not empty, since otherwise they will have
                # no columns
                town_df = (
                    model.df
                    if model.df.empty
                    else model.df[
                        model.df["township_code"] == town.township_code
                    ]
                )
                town_export_filename = (
                    f"{town.township_name} {model.export_filename}"
                )
                town_model_args = {
                    **dataclasses.asdict(model),
                    "df": town_df,
                    "export_filename": town_export_filename,
                }
                town_model = ModelForExport(**town_model_args)

                # Save query results to file, with a dedicated subdirectory
                # for each town if we're exporting more than one town
                print(f"Exporting {model.name} for {town.township_name}")
                town_output_dirpath = (
                    pathlib.Path(output_dir)
                    if len(townships) < 2
                    else pathlib.Path(output_dir) / town.township_name
                )
                town_output_dirpath.mkdir(parents=True, exist_ok=True)
                save_model_to_workbook(
                    town_model, town_output_dirpath.as_posix()
                )


if __name__ == "__main__":
    main()
