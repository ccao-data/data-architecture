# Export town close QC reports to Excel files.
#
# Run `python scripts/export_qc_town_close_reports.py --help` for details.
import argparse
import datetime
import os
import sys

from dbt.cli.main import dbtRunner

# Add the parent directory of `scripts` to the module search path
# so that we can import from other modules in the `scripts` directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import constants
from scripts.export_models import export_models

DBT = dbtRunner()

CLI_DESCRIPTION = f"""Export town close QC reports to Excel files.

Expects dependencies from requirements.txt (dbt dependencies) and scripts/requirements.export_models.txt (script dependencies) be installed.

{constants.REFRESH_TABLES_DESCRIPTION}
"""  # noqa: E501
CLI_EXAMPLE = """Example usage to output the 2024 town close QC report for Hyde Park:

    python scripts/export_qc_town_close_reports.py --township 70 --year 2024

To output the town close QC report for Hyde Park in the current year:

    python scripts/export_qc_town_close_reports.py --township 70

To print a command you can run to refresh iasWorld tables prior to export:

    python scripts/export_qc_town_close_reports.py --township 70 --year 2024 --refresh-tables
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
    tri = TOWNSHIP_TO_TRI[township_code]
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
        *constants.TARGET_CLI_ARGS, **constants.TARGET_CLI_KWARGS
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
        *constants.REBUILD_CLI_ARGS, **constants.REBUILD_CLI_KWARGS
    )
    parser.add_argument(
        *constants.REFRESH_TABLES_CLI_ARGS,
        **constants.REFRESH_TABLES_CLI_KWARGS,
    )

    return parser.parse_args()


def main():
    """Main entrypoint for the script"""
    args = parse_args()

    tag = "tag:qc_report_town_close"
    tag_suffix = "tri" if is_tri(args.township, args.year) else "non_tri"
    where = f"township_code = '{args.township}' and taxyr = '{args.year}'"

    export_models(
        target=args.target,
        rebuild=args.rebuild,
        select=[tag, f"{tag}_{tag_suffix}"],
        where=where,
        refresh_tables=args.refresh_tables,
    )


if __name__ == "__main__":
    main()
