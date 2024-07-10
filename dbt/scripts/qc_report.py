# Generate QC reports
import argparse
import contextlib
import io
import json
import os
import shutil

import pandas as pd
import pyathena
from dbt.cli.main import dbtRunner
from openpyxl.utils import get_column_letter

DBT = dbtRunner()


def main():
    parser = argparse.ArgumentParser(description="Output a set of QC reports")
    parser.add_argument(
        "--select",
        required=True,
        nargs="*",
        help="One or more dbt select statements to use for filtering reports",
    )
    parser.add_argument(
        "--target",
        required=False,
        default="dev",
        help="dbt target to use for querying data, defaults to 'dev'",
    )

    args = parser.parse_args()
    target = args.target
    select = args.select

    print(f"Checking for models matching these select statements: {select}")
    dbt_output = io.StringIO()
    with contextlib.redirect_stdout(dbt_output):
        dbt_result = DBT.invoke(
            [
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
                "--select",
                *select,
            ]
        )

    if not dbt_result.success:
        print("Encountered error in dbt call")
        raise ValueError(dbt_result.exception)

    # Output is formatted as a list of newline-separated JON objects
    models = [
        json.loads(model_dict_str)
        for model_dict_str in dbt_output.getvalue().split("\n")
        # Filter out empty strings caused by trailing newlines
        if model_dict_str
    ]

    if not models:
        raise ValueError(f"No models found for the --select value '{select}'")

    conn = pyathena.connect(
        s3_staging_dir=os.getenv(
            "AWS_ATHENA_S3_STAGING_DIR",
            "s3://ccao-dbt-athena-results-us-east-1",
        ),
        region_name=os.getenv("AWS_ATHENA_REGION_NAME", "us-east-1"),
    )

    for model in models:
        # Extract useful model metadata
        model_name = model["name"]
        relation_name = model["relation_name"]
        report_name = model["config"]["meta"].get("report_name")
        if not report_name:
            raise ValueError(
                f"Model {model_name} is missing required meta.report_name "
                "attribute"
            )

        # Define inputs and outputs for report based on model metadata
        template_path = os.path.join(
            "reports", "templates", f"{model_name}.xlsx"
        )
        if not template_path:
            raise ValueError(
                f"Missing expected report template at {template_path}"
            )
        output_path = os.path.join("reports", "output", f"{report_name}.xlsx")

        # Query Athena for report data
        print(f"Querying data for model {model_name}")
        model_df = pd.read_sql(f"SELECT * FROM {relation_name}", conn)

        # Write report data to Excel
        shutil.copyfile(template_path, output_path)
        writer = pd.ExcelWriter(
            output_path, engine="openpyxl", mode="a", if_sheet_exists="overlay"
        )
        # TODO: Support sheet name customization
        sheet_name = "Query Results"
        model_df.to_excel(
            writer,
            sheet_name="Query Results",
            header=False,
            index=False,
            startrow=1,
        )
        # Adjust extent of data table for filtering
        sheet = writer.sheets[sheet_name]
        writer.sheets[sheet_name].tables[
            "Query_Results"
        ].ref = f"A1:{get_column_letter(sheet.max_column)}{str(sheet.max_row)}"
        writer.close()
        print(f"Wrote report from model {model_name} to {output_path}")


if __name__ == "__main__":
    main()
