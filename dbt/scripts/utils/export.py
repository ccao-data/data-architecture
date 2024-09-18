# Shared utilities for exporting models
import contextlib
import io
import json
import os
import pathlib
import shutil

import pandas as pd
import pyathena
from dbt.cli.main import dbtRunner
from openpyxl.styles import Alignment
from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

# Shared object for running dbt CLI commands
DBT = dbtRunner()


def export_models(
    target: str = "dev",
    select: list[str] | None = None,
    selector: str | None = None,
    rebuild: bool = False,
    where: str | None = None,
):
    """
    Export a group of models to Excel workbooks in the output directory
    `export/output/`.

    Arguments:

        * target (str): dbt target to use for querying model data, defaults to
            "dev"
        * select (list[str]): One or more dbt --select statements to
            use for filtering models
        * selector (str): A selector name to use for filtering
            models, as defined in selectors.yml. One of `select` or `selector`
            must be set, but they can't both be set
        * rebuild (bool): Rebuild models before exporting, defaults to False
        * where (str): Optional SQL expression representing a WHERE clause to
            filter models
    """
    if not select and not selector:
        raise ValueError("One of --select or --selector is required")

    if select and selector:
        raise ValueError("--select and --selector cannot both be set")

    select_args = ["--select", *select] if select else ["--selector", selector]  # type: ignore

    if rebuild:
        dbt_run_args = ["run", "--target", target, *select_args]
        print("Rebuilding models")
        print(f"> dbt {' '.join(dbt_run_args)}")
        dbt_run_result = DBT.invoke(dbt_run_args)
        if not dbt_run_result.success:
            print("Encountered error in `dbt run` call")
            raise ValueError(dbt_run_result.exception)

    print("Listing models to select for export")
    dbt_list_args = [
        "--quiet",
        "list",
        "--target",
        target,
        "--resource-types",
        "model",
        "--output",
        "json",
        "--output-keys",
        "name",
        "config",
        "relation_name",
        *select_args,
    ]
    print(f"> dbt {' '.join(dbt_list_args)}")
    dbt_output = io.StringIO()
    with contextlib.redirect_stdout(dbt_output):
        dbt_list_result = DBT.invoke(dbt_list_args)

    if not dbt_list_result.success:
        print("Encountered error in `dbt list` call")
        raise ValueError(dbt_list_result.exception)

    # Output is formatted as a list of newline-separated JSON objects
    models = [
        json.loads(model_dict_str)
        for model_dict_str in dbt_output.getvalue().split("\n")
        # Filter out empty strings caused by trailing newlines
        if model_dict_str
    ]

    if not models:
        raise ValueError(
            f"No models found for the select option '{' '.join(select_args)}'"
        )

    print(
        "The following models will be exported: "
        f"{', '.join(model['name'] for model in models)}"
    )

    conn = pyathena.connect(
        s3_staging_dir=os.getenv(
            "AWS_ATHENA_S3_STAGING_DIR",
            "s3://ccao-dbt-athena-results-us-east-1",
        ),
        region_name=os.getenv("AWS_ATHENA_REGION_NAME", "us-east-1"),
    )

    for model in models:
        # Extract useful model metadata from the columns we queried in
        # the `dbt list` call above
        model_name = model["name"]
        relation_name = model["relation_name"]
        export_name = model["config"]["meta"].get("export_name") or model_name
        template = model["config"]["meta"].get("export_template") or model_name

        # Define inputs and outputs for export based on model metadata
        template_path = os.path.join("export", "templates", f"{template}.xlsx")
        template_exists = os.path.isfile(template_path)
        output_path = os.path.join("export", "output", f"{export_name}.xlsx")

        print(f"Querying data for model {model_name}")
        query = f"SELECT * FROM {relation_name}"
        if where:
            query += f" WHERE {where}"
        print(f"> {query}")
        model_df = pd.read_sql(query, conn)

        # Delete the output file if one already exists
        pathlib.Path(output_path).unlink(missing_ok=True)

        if template_exists:
            print(f"Using template file at {template_path}")
            shutil.copyfile(template_path, output_path)
        else:
            print("No template file exists; creating a workbook from scratch")

        writer_kwargs = (
            {"mode": "a", "if_sheet_exists": "overlay"}
            if template_exists
            else {}
        )
        with pd.ExcelWriter(
            output_path, engine="openpyxl", **writer_kwargs
        ) as writer:
            sheet_name = "Sheet1"
            model_df.to_excel(
                writer,
                sheet_name=sheet_name,
                header=False if template_exists else True,
                index=False,
                startrow=1 if template_exists else 0,
            )
            sheet = writer.sheets[sheet_name]

            # Add a table for data filtering. Only do this if the result set
            # is not empty, because otherwise the empty table will make
            # the Excel workbook invalid
            if model_df.empty:
                print(
                    "Skipping formatting for output workbook since result set "
                    "is empty"
                )
            else:
                table = Table(
                    displayName="Query_Results",
                    ref=(
                        f"A1:{get_column_letter(sheet.max_column)}"
                        f"{str(sheet.max_row)}"
                    ),
                )
                table.tableStyleInfo = TableStyleInfo(
                    name="TableStyleMedium11", showRowStripes=True
                )
                sheet.add_table(table)

                # Parse column format settings by col index. Since column
                # formatting needs to be applied at the cell level, we'll
                # first parse all format settings for each column, and then
                # we'll iterate every cell once to apply all formatting at the
                # same time
                column_format_by_index = {}

                # If a parid column exists, format it explicitly as a
                # 14-digit number to avoid Excel converting it to scientific
                # notation or stripping out leading zeros
                if "parid" in model_df or "pin" in model_df:
                    parid_field = "parid" if "parid" in model_df else "pin"
                    parid_index = model_df.columns.get_loc(parid_field)
                    column_format_by_index[parid_index] = {
                        "number_format": "00000000000000",
                        # Left align since PINs do not actually need to be
                        # compared by order of magnitude the way that numbers
                        # do
                        "alignment": Alignment(horizontal="left"),
                    }

                # Parse any formatting that is configured at the column level.
                # Note that if formatting is configured for a column that we
                # parsed as a parid column above, these settings will override
                # the default parid settings from the block above
                format_config = model["config"]["meta"].get(
                    "export_format", {}
                )
                if column_configs := format_config.get("columns"):
                    for column_config in column_configs:
                        # The column index is required in order to set any
                        # column-level configs
                        col_letter = column_config.get("index")
                        if col_letter is None:
                            raise ValueError(
                                "'index' attribute is required in "
                                "export_format.columns config for "
                                f"model {model_name}"
                            )
                        idx = column_index_from_string(col_letter) - 1
                        # Initialize the config dict for this column if
                        # none exists yet
                        column_format_by_index[idx] = {}
                        # Parse configs if they are present
                        if number_format := column_config.get("number_format"):
                            column_format_by_index[idx]["number_format"] = (
                                number_format
                            )
                        if horiz_align_dir := column_config.get(
                            "horizontal_align"
                        ):
                            column_format_by_index[idx]["alignment"] = (
                                Alignment(horizontal=horiz_align_dir)
                            )

                # Skip header row when applying formatting. We need to
                # catch the special case where there is only one row, or
                # else we will iterate the _cells_ in that row instead of
                # the row when slicing it from 2 : max_row
                non_header_rows = (
                    [sheet[2]]
                    if sheet.max_row == 2
                    else sheet[2 : sheet.max_row]
                )
                for row in non_header_rows:
                    for idx, formats in column_format_by_index.items():
                        for attr, val in formats.items():
                            setattr(row[idx], attr, val)

        print(f"Exported model {model_name} to {output_path}")
