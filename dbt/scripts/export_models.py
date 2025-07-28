# Export dbt models to Excel files.
#
# Run `python scripts/export_models.py --help` for details.
import argparse
import logging
from datetime import date

from utils import constants
from utils.aws import upload_logs_to_cloudwatch
from utils.export import export_models

# Create and start the logger
logger = logging.getLogger(__name__)


CLI_DESCRIPTION = """Export dbt models to Excel files.

Expects dependencies from [project].dependencies (dbt dependencies) and [project.optional-dependencies].dbt_tests (script dependencies) be installed.

A few configuration values can be set on any model to support exporting:

    * config.meta.export_name (optional): The base name of the output file that will be generated. File extensions are not necessary since all
      models are exported as .xlsx files. If unset, defaults to the name of the model.

    * config.meta.export_template (optional): Configs that apply to an optional Excel template that the script will populate with data. Attributes include:
        * name (optional): The base name of the template file. File extensions are not necessary since all templates are assumed to be .xlsx files. Templates should
          be stored in the export/templates/ directory and should include header rows. If unset, will search for a template with the same name as the
          model; if no template is found or if the attribute is not present, defaults to a simple layout with filterable columns and striped rows.
        * start_row (optional): The 1-indexed position of the row in the sheet that should be the first non-header row, i.e. the start row for the data.
        * add_table (optional): Whether to add a data table for sorting and filtering. Defaults to True.
        * sheet_name (optional): The name of the sheet in the workbook to populate with data. Defaults to "Sheet1".

    * config.meta.export_format (optional): Formatting to apply to the output workbook. Useful for specific types of formatting, like alignment
      and number formats, that Excel can only apply after populating a template with data
        * format_blanks_as_empty_string (optional): When True, indicates to the script to export blanks as empty strings instead of nulls. Defaults to False.
"""  # noqa: E501
CLI_EXAMPLE = """Example usage to output the 2024 non-tri town close QC report for Leyden, which is a non-tri town in 2024:

    python scripts/export_models.py --selector select_qc_report_town_close_non_tri --where "township_code = '20' and taxyr = '2024'"

To output the 2024 tri town close QC report for Hyde Park, which is a tri town in 2024:

    python scripts/export_models.py --selector select_qc_report_town_close_tri --where "township_code = '70' and taxyr = '2024'"

To output the 2024 AHSAP property report for Hyde Park:

    python3 scripts/export_models.py --select qc.vw_change_in_ahsap_values --where "township_code = '70' and taxyr = '2024'"
"""  # noqa: E501


def parse_args():
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
        *constants.SELECT_ARGUMENT_ARGS, **constants.SELECT_ARGUMENT_KWARGS
    )
    parser.add_argument(
        *constants.SELECTOR_ARGUMENT_ARGS, **constants.SELECTOR_ARGUMENT_KWARGS
    )
    parser.add_argument(
        "--where",
        required=False,
        help="SQL expression representing a WHERE clause to filter models",
    )
    parser.add_argument(
        *constants.OUTPUT_DIR_ARGUMENT_ARGS,
        **constants.OUTPUT_DIR_ARGUMENT_KWARGS,
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(filename="export_models.log", level=logging.INFO)
    logger.info("Starting export_models.py script")

    args = parse_args()

    try:
        export_models(
            args.target,
            args.select,
            args.selector,
            args.rebuild,
            args.where,
            args.output_dir,
        )

        logger.info("Export completed successfully.")

    except Exception as e:
        logger.error(e)

    upload_logs_to_cloudwatch(
        log_group_name="/ccao/jobs/ic_reference_file_export",
        log_stream_name=f"daily_export_{date.today()}",
        log_file_path="models.log",
    )
